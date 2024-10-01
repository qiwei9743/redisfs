use std::error::Error;
use std::fs::OpenOptions;

use std::ffi::OsStr;
use libc::{ENOENT, EIO};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use slog::{debug, error, Logger, o, Drain};
use slog_term;
use slog_async;
use fuser::FileAttr;
use slog_envlogger;
use std::sync::Arc;
use libc::{EINVAL, ENOTEMPTY};
use std::collections::HashMap;
use std::time::SystemTime;
use fuser::FileType;
use futures::future::join_all;
use futures::Future;


mod metadata;

pub use metadata::FileOptionAttr;

pub struct RedisFs {
    redis_client: ConnectionManager,
    pub logger: Arc<Logger>,
    block_size: u64,
}

impl Clone for RedisFs {
    fn clone(&self) -> Self {
        RedisFs {
            redis_client: self.redis_client.clone(),
            logger: Arc::clone(&self.logger),
            block_size: self.block_size,
        }
    }
}

fn offset2blockno(offset: u64, block_size: u64) -> u64 {
    (offset as u64) / block_size
}

async fn write_block(conn: &ConnectionManager, logger: &Logger, 
        offset: u64, ino: u64, data: &[u8], offset_in_block: u64, read_existing: bool, block_size: u64, file_size: u64) -> Result<(u64, bool), i32> {
    let block_key = metadata::inode_block_key(ino, offset / block_size);
    let is_new_block: bool;

    if data.len() == block_size as usize && offset_in_block == 0 {
        // 使用 file_size 来判断块是否存在
        is_new_block = offset >= file_size;

        conn.clone().set(block_key.as_str(), data).await.map_err(|e| {
            slog::error!(logger, "Failed to write full block"; "inode" => ino, "error" => ?e);
            EIO
        })?;
        slog::debug!(logger, "Wrote block"; "inode" => ino, "blockno" => offset / block_size, "block_key" => block_key.as_str(), "offset" => offset, "block_size" => block_size,
            "data_len" => data.len(), "new_block_data" => format!("{:?}", data));
        return Ok((data.len() as u64, is_new_block));
    }

    let mut old_block = if read_existing {
        let result: redis::RedisResult<Option<Vec<u8>>> = conn.clone().get(block_key.as_str()).await;
        match result {
            Ok(block) => {
                is_new_block = block.is_none();
                block.unwrap_or(vec![0; block_size as usize])
            },
            Err(e) => {
                slog::error!(logger, "Failed to read file block"; "inode" => ino, "blockno" => offset / block_size, "error" => ?e, "function" => "write_partial_block2");
                return Err(EIO);
            }
        }
    } else {
        is_new_block = offset >= file_size;
        vec![0; offset_in_block as usize]
    };


    if old_block.len() < offset_in_block as usize + data.len() {
        old_block.resize(offset_in_block as usize + data.len(), 0);
    }

    old_block.splice(offset_in_block as usize..offset_in_block as usize+ data.len(), 
        data.iter().cloned());

    let new_block = old_block;
    slog::debug!(logger, "Writing block"; "inode" => ino, "blockno" => offset / block_size, "block_key" => block_key.as_str(), "offset" => offset, "block_size" => block_size,
        "data_len" => data.len(), "new_block_len" => new_block.len(), "read_existing" => read_existing, "offset_in_block" => offset_in_block,
        "new_block_data" => format!("{:?}", new_block.as_slice()));
    conn.clone().set(block_key.as_str(), new_block).await.map_err(|e| {
        slog::error!(logger, "Failed to write file block"; "inode" => ino, "error" => ?e, "function" => "write_partial_block");
        EIO
    })?;

    Ok((data.len() as u64, is_new_block))
}


impl RedisFs {
    pub async fn new(redis_url: &str, block_size: u64) -> Result<Self, Box<dyn Error>> {
        println!("Attempting to connect to Redis at {}", redis_url);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("log.txt")?;

        // Create a terminal-style Drain
        let decorator_term = slog_term::TermDecorator::new().build();
        let drain_term = slog_term::FullFormat::new(decorator_term).build().fuse();
        let drain_term = slog_async::Async::new(drain_term).build().fuse();

        // Create a file-style Drain
        let decorator_file = slog_term::PlainDecorator::new(file);
        let drain_file = slog_term::FullFormat::new(decorator_file).build().fuse();
        let drain_file = slog_async::Async::new(drain_file).build().fuse();

        // Merge the two Drains
        let drain = slog::Duplicate::new(drain_term, drain_file).fuse();
        
        // Create a configurable log level using slog_envlogger
        let drain = slog_envlogger::new(drain).fuse();
        
        // Create the root logger
        let logger = Arc::new(slog::Logger::root(drain, o!("module" => "redisfs")));

        println!("Redis client created successfully");
        let client = redis::Client::open(redis_url)?;
        println!("Connection manager creation attempt");
        let connection_manager = ConnectionManager::new(client).await?;
        println!("Connection manager created successfully");
        let redis_client = connection_manager;

        let fs = RedisFs { 
            redis_client,
            logger,
            block_size,
        };
        match fs.ensure_root_inode().await {
            Ok(_) => println!("Root inode ensured successfully"),
            Err(e) => {
                eprintln!("Failed to ensure root inode: {:?}", e);
                return Err(e);
            }
        }
        println!("RedisFs instance created successfully");
        Ok(fs)
    }

    async fn ensure_root_inode(&self) -> Result<(), Box<dyn Error>> {
        let mut conn = self.redis_client.clone();
        let root_key = metadata::inode_attr_key(1);

        if !conn.exists(&root_key).await? {
            debug!(self.logger, "Root inode does not exist, creating"; "function" => "ensure_root_inode");

            let now = SystemTime::now();

            let attr = FileAttr {
                ino: 1,
                size: 0,
                blocks: 1,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: self.block_size as u32,
                flags: 0,
            }.into();
            
            self.set_attr(1, &attr).await
                .map_err(
                    |e| 
                    Box::new(std::io::Error::from_raw_os_error(e)) as Box<dyn Error>)?;

            conn.set("next_inode", 2).await?;

            // Create . and .. entries for root directory
            let children_key = metadata::inode_children_key(1);
            conn.hset(&children_key, ".", 1).await?;
            conn.hset(&children_key, "..", 1).await?;

            debug!(self.logger, "Root inode created, next_inode set to 2"; "function" => "ensure_root_inode");
        } else {
            debug!(self.logger, "Root inode already exists"; "function" => "ensure_root_inode");
            // Verify and correct root inode if necessary
            let attr: HashMap<String, String> = conn.hgetall(&root_key).await?;
            if attr.get("kind").map_or(true, |k| k != "4") || 
               attr.get("mode").map_or(true, |m| m != "0755") ||
               attr.get("nlink").map_or(true, |n| n != "2") {
                // Correct the root inode attributes
                conn.hset(&root_key, "kind", "4").await?;
                conn.hset(&root_key, "mode", "0755").await?;
                conn.hset(&root_key, "nlink", "2").await?;
                debug!(self.logger, "Corrected root inode attributes"; "function" => "ensure_root_inode");
            }
        }

        Ok(())
    }


    pub async fn set_attr(&self, ino: u64, attr: &FileOptionAttr) -> Result<(), i32> {
        let mut conn = self.redis_client.clone();

        let attr_key = metadata::inode_attr_key(ino);
        let attr_map = attr.to_redis_hash();
        let attr: Vec<(&String, &String)> = attr_map.iter().collect();
        conn.hset_multiple(&attr_key, &attr).await.map_err(|e| {
            slog::error!(self.logger, "Failed to set inode attributes"; "inode" => ino, "error" => ?e, "function" => "set_attr");
            EIO
        })?;
        Ok(())
    }


    pub async fn get_attr(&self, ino: u64) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();

        let attr_key = metadata::inode_attr_key(ino);
        let attr: HashMap<String, String> = conn.hgetall(&attr_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get inode attributes"; "inode" => ino, "error" => ?e, "function" => "get_attr");
            EIO
        })?;

        if attr.is_empty() {
            slog::error!(self.logger, "Inode does not exist"; "inode" => ino, "function" => "get_attr");
            return Err(ENOENT);
        }

        let attr = metadata::FileOptionAttr::from(attr).into();

        slog::debug!(self.logger, "Successfully got inode attributes"; "inode" => ino, "attr" => ?attr, "function" => "get_attr");
        Ok(attr)
    }

    pub async fn lookup(&self, parent: u64, name: &OsStr) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();

        let parent_key = metadata::inode_children_key(parent);
        let child_ino: Result<u64, redis::RedisError> = conn.hget(&parent_key, name.to_str().unwrap()).await;

        match child_ino {
            Ok(ino) => {
                match self.get_attr(ino).await {
                    Ok(attr) => Ok(attr),
                    Err(e) => {
                        error!(self.logger, "Failed to get attributes"; "inode" => ino, "error" => ?e, "function" => "lookup");
                        Err(EIO)
                    }
                }
            },
            Err(_) => Err(ENOENT),
        }
    }

    // Helper function for permission check
    fn check_permission(&self, attr: &FileAttr, uid: u32, gid: u32, mask: u32) -> bool {
        if uid == 0 {  // root user always has permission
            return true;
        }

        let mode = if attr.uid == uid {
            (attr.perm >> 6) & 7
        } else if attr.gid == gid {
            (attr.perm >> 3) & 7
        } else {
            attr.perm & 7
        };

        (mode as u32 & mask) == mask
    }

    pub async fn read_file(&self, ino: u64, offset: i64, size: u32, uid: u32, gid: u32) -> Result<Vec<u8>, i32> {
        let attr = self.get_attr(ino).await?;
        
        if !self.check_permission(&attr, uid, gid, 4) {  // 4 is for read permission
            slog::warn!(self.logger, "Insufficient permissions to read file"; "inode" => ino, "uid" => uid, "gid" => gid, "function" => "read_file");
            return Err(libc::EACCES);
        }

        let mut conn = self.redis_client.clone();
        let file_size = attr.size;

        slog::debug!(self.logger, "Reading file"; 
            "function" => "read_file",
            "inode" => ino, 
            "offset" => offset,
            "read_size" => size,
            "file_size" => file_size
        );

        if offset as u64 >= file_size {
            return Ok(Vec::new());
        }

        let start_block = (offset as u64) / self.block_size;
        let end_block = ((offset as u64 + size as u64 - 1) / self.block_size).min((file_size - 1) / self.block_size);
        slog::debug!(self.logger, "Reading file blocks"; "start_block" => start_block, "end_block" => end_block, "function" => "read_file");
        
        let mut data = Vec::new();
        for block in start_block..=end_block {
            let block_key = metadata::inode_block_key(ino, block);
            let block_data: Option<Vec<u8>> = conn.get(&block_key).await.map_err(|e| {
                slog::error!(self.logger, "Failed to read file block"; "inode" => ino, "block" => block, "error" => ?e, "function" => "read_file");
                EIO
            })?;

            let mut block_data = block_data.unwrap_or_else(|| Vec::new());
            
            // 如果块的实际大小小于预期的块大小，用零填充
            if block_data.len() < self.block_size as usize {
                block_data.resize(self.block_size as usize, 0);
            }

            let block_offset = block * self.block_size;
            let start = if block == start_block {
                offset as u64 - block_offset
            } else {
                0
            } as usize;

            let end = if block == end_block {
                ((offset as u64 + size as u64).min(file_size) - block_offset) as usize
            } else {
                self.block_size as usize
            };

            data.extend_from_slice(&block_data[start..end]);

            slog::debug!(self.logger, "Read block"; 
                "function" => "read_file",
                "inode" => ino,
                "block" => block,
                "block_data_len" => block_data.len(),
                "start" => start,
                "end" => end,
                "data_len" => data.len()
            );

            if data.len() as u64 >= size as u64 {
                break;
            }
        }

        // Trim the data if we've read more than requested
        let read_size = (size as u64).min(file_size - offset as u64);
        if data.len() as u64 > read_size {
            slog::debug!(self.logger, "Truncating read data"; 
                "function" => "read_file",
                "inode" => ino,
                "original_size" => data.len(),
                "truncated_size" => read_size
            );
            data.truncate(read_size as usize);
        }

        slog::debug!(self.logger, "Successfully read file"; "inode" => ino, "offset" => offset, "bytes_read" => data.len(), "function" => "read_file");
        Ok(data)
    }

    pub async fn write_file_blocks(&self, ino: u64, offset1: i64, data1: &[u8], uid: u32, gid: u32) -> Result<u64, i32> {
        let mut data = data1;
        let mut offset = offset1;
        let attr = self.get_attr(ino).await?;
        let conn = self.redis_client.clone();
        if !self.check_permission(&attr, uid, gid, 2) {
            slog::warn!(self.logger, "Insufficient permissions to write file"; "inode" => ino, "uid" => uid, "gid" => gid, "function" => "write_file_blocks2");
            return Err(libc::EACCES);
        }

        let mut futures_vec = Vec::new();
        let file_size = attr.size;

        if offset as u64 % self.block_size != 0  {
            // handle first block if it's not ful filled a block.
            let offset_in_block = offset as u64 % self.block_size;
            let write_size = std::cmp::min(data.len(), self.block_size as usize - offset_in_block as usize);
            let read_existing = attr.size > 0 && offset2blockno(offset as u64 + write_size as u64 - 1, self.block_size) <= offset2blockno(attr.size - 1, self.block_size);
            let r= write_block(&conn, &self.logger, offset as u64, ino,
                &data[..write_size], offset_in_block, read_existing, self.block_size, file_size);
            futures_vec.push(r);

            offset = offset + write_size as i64;
            data = &data[write_size..];
        }

        while data.len() >= self.block_size as usize {
            let read_existing = attr.size > 0 && offset2blockno(offset as u64 + self.block_size as u64 - 1, self.block_size) <= offset2blockno(attr.size - 1, self.block_size);
            let r = write_block(&conn, &self.logger, offset as u64, ino,
                &data[..self.block_size as usize], 0, read_existing, self.block_size, file_size);
            
            futures_vec.push(r);
            offset += self.block_size as i64;
            data = &data[self.block_size as usize..];
        }
        if data.len() > 0 {
            // handle last block
            let read_existing = attr.size > 0 && offset2blockno(offset as u64 + data.len() as u64 - 1, self.block_size) <= offset2blockno(attr.size - 1, self.block_size);
            slog::debug!(self.logger, "Writing last block"; "function" => "write_file_blocks", "inode" => ino, "offset" => offset, "data_len" => data.len(), "read_existing" => read_existing);
            let r = write_block(&conn, &self.logger, offset as u64, ino,
                &data[..], 0, read_existing, self.block_size, file_size);
            futures_vec.push(r);
        }

        slog::debug!(self.logger, "Executing write futures"; "function" => "write_file_blocks", "future_cnt" => futures_vec.len());
        let (write_bytes, blocks_delta) = self.execute_write_futures(futures_vec).await?;
        assert!(write_bytes == data1.len() as u64, "write_bytes: {}, data_len: {}", write_bytes, data1.len());

        let new_size = if attr.size < offset1 as u64 + write_bytes {
            Some(offset1 as u64 + write_bytes)
        } else {
            None
        };

    
        let now = std::time::SystemTime::now();
        let attr_opt = FileOptionAttr {
            size: new_size,
            mtime: Some(now),
            ctime: Some(now),  // 同时更新 mtime 和 ctime
            blocks: if blocks_delta != 0 {
                Some((attr.blocks as i64 + blocks_delta).max(0) as u64)
            } else {
                None
            },
            ..Default::default()
        };
        
        self.set_attr(ino, &attr_opt).await?;
    
    
        Ok(write_bytes)
    }

    async fn execute_write_futures(&self, futures: Vec<impl Future<Output = Result<(u64, bool), i32>>>) -> Result<(u64, i64), i32> {
        let mut total_bytes = 0;
        let mut blocks_delta = 0;

        for future in futures {
            let (bytes, is_new_block) = future.await?;
            total_bytes += bytes;
            if is_new_block {
                blocks_delta += 1;
            }
        }

        Ok((total_bytes, blocks_delta))
    }

    async fn update_file_attributes(&self, ino: u64, attr: FileAttr, offset: i64, bytes_written: u64) -> Result<(), i32> {
        let new_size = attr.size.max(offset as u64 + bytes_written);
        let new_mtime = std::time::SystemTime::now();
        let attr = FileOptionAttr {
            size: Some(new_size),
            mtime: Some(new_mtime),
            ..Default::default()
        };
        self.set_attr(ino, &attr).await?;

        Ok(())
    }

    pub async fn create_file(&self, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, uid: u32, gid: u32) -> Result<(u64, FileAttr), i32> {
        let parent_attr = self.get_attr(parent).await?;

        if !self.check_permission(&parent_attr, uid, gid, 2) {
            slog::warn!(self.logger, "Insufficient permissions to create file"; "parent_inode" => parent, "uid" => uid, "gid" => gid, "function" => "create_file");
            return Err(libc::EACCES);
        }

        let mut conn = self.redis_client.clone();

        // Generate new inode number
        let new_ino: u64 = conn.incr("next_inode", 1).await.map_err(|e| {
            error!(self.logger, "Failed to generate new inode number"; "error" => ?e, "function" => "create_file");
            EIO
        })?;

        // Create attributes for the new file
        let now = std::time::SystemTime::now();
        
        let attr = FileAttr {
            ino: new_ino,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: fuser::FileType::RegularFile,
            perm: (mode & !umask) as u16,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            flags: flags as u32,
            blksize: self.block_size as u32,
        };

    
        let attr_opt = attr.into();
        self.set_attr(new_ino, &attr_opt).await?;

        // Add the new file to the parent directory
        let parent_key = metadata::inode_children_key(parent);
        conn.hset(&parent_key, name.to_str().unwrap(), new_ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new file to parent directory"; "parent_inode" => parent, "file_name" => ?name, "error" => ?e, "function" => "create_file");
            EIO
        })?;

        slog::info!(self.logger, "Successfully created new file"; "parent_inode" => parent, "file_name" => ?name, "new_inode" => new_ino, "function" => "create_file");
        Ok((new_ino, attr))
    }

    pub async fn create_directory(&self, parent: u64, name: &OsStr, mode: u32, umask: u32, uid: u32, gid: u32) -> Result<(u64, FileAttr), i32> {
        let parent_attr = self.get_attr(parent).await?;
        
        if !self.check_permission(&parent_attr, uid, gid, 2) {
            slog::warn!(self.logger, "Insufficient permissions to create directory"; "parent_inode" => parent, "uid" => uid, "gid" => gid, "function" => "create_directory");
            return Err(libc::EACCES);
        }

        let mut conn = self.redis_client.clone();

        // Generate new inode number
        let new_ino: u64 = conn.incr("next_inode", 1).await.map_err(|e| {
            slog::error!(self.logger, "Failed to generate new inode number"; "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        // Create attributes for the new directory
        let now = std::time::SystemTime::now();
        let attr = FileOptionAttr {
            ino: Some(new_ino),
            size: Some(0),
            blocks: Some(0),
            atime: Some(now),
            mtime: Some(now),
            ctime: Some(now),
            crtime: Some(now),
            kind: Some(fuser::FileType::Directory),
            perm: Some((mode & !umask) as u16),
            nlink: Some(1),
            uid: Some(uid),
            gid: Some(gid),
            rdev: Some(0),
            blksize: Some(self.block_size as u32),
            flags: None,
        };
        // Set attributes for the new directory
        self.set_attr(new_ino, &attr).await.map_err(|e| {
            slog::error!(self.logger, "Failed to set new directory attributes"; "inode" => new_ino, "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        // Add the new directory to the parent directory
        let parent_key = metadata::inode_children_key(parent);
        conn.hset(&parent_key, name.to_str().unwrap(), new_ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new directory to parent"; "parent_inode" => parent, "dir_name" => ?name, "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        // Create . and .. entries for the new directory
        let children_key = metadata::inode_children_key(new_ino);
        let mut pipe = redis::pipe();
        pipe.hset(&children_key, ".", new_ino)
            .hset(&children_key, "..", parent);

        pipe.query_async(&mut conn).await.map_err(|e| {
            slog::error!(self.logger, "Failed to create . and .. entries for new directory"; "inode" => new_ino, "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        slog::info!(self.logger, "Successfully created new directory"; "parent_inode" => parent, "dir_name" => ?name, "new_inode" => new_ino, "function" => "create_directory");
        Ok((new_ino, attr.into()))
    }

    pub async fn read_dir(&self, parent: u64, offset: i64) -> Result<Vec<(u64, std::ffi::OsString, FileAttr)>, i32> {
        use std::ffi::OsString;

        let mut conn = self.redis_client.clone();

        let children_key = metadata::inode_children_key(parent);
        let children: Vec<(String, u64)> = conn.hgetall(&children_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to read directory contents"; "parent_inode" => parent, "error" => ?e, "function" => "read_dir");
            EIO
        })?;

        let mut entries = Vec::new();
        for (name, ino) in children.into_iter().skip(offset as usize) {
            match self.get_attr(ino).await {
                Ok(attr) => entries.push((ino, OsString::from(name), attr)),
                Err(e) => {
                    slog::error!(self.logger, "Failed to get file attributes"; "inode" => ino, "error" => ?e, "function" => "read_dir");
                    continue; // Skip this entry and continue processing others
                }
            }
        }

        slog::debug!(self.logger, "Successfully read directory contents"; "parent_inode" => parent, "entry_count" => entries.len(), "offset" => offset, "function" => "read_dir");
        Ok(entries)
    }

    pub async fn unlink(&self, parent: u64, name: &OsStr, uid: u32, gid: u32) -> Result<(), i32> {
        let mut conn = self.redis_client.clone();
        let parent_key = metadata::inode_children_key(parent);
        let name_str = name.to_str().ok_or(EINVAL)?;

        // Check permissions on the parent directory
        let parent_attr = self.get_attr(parent).await?;
        if !self.check_permission(&parent_attr, uid, gid, 2) {  // 2 is for write permission
            slog::warn!(self.logger, "Insufficient permissions to unlink file"; "parent_inode" => parent, "uid" => uid, "gid" => gid, "function" => "unlink");
            return Err(libc::EACCES);
        }

        // Get file's inode
        let ino: u64 = conn.hget(&parent_key, name_str).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get file inode"; "parent_inode" => parent, "file_name" => ?name, "error" => ?e, "function" => "unlink");
            EIO
        })?;

        // Get the file's attributes
        let mut attr = self.get_attr(ino).await?;

        // Decrease the nlink count
        attr.nlink -= 1;

        if attr.nlink == 0 {
            // If nlink is 0, delete the file data and attributes
            let file_key = metadata::inode_data_key(ino);
            conn.del(&file_key).await.map_err(|e| {
                slog::error!(self.logger, "Failed to delete file data"; "inode" => ino, "error" => ?e, "function" => "unlink");
                EIO
            })?;

            let attr_key = metadata::inode_attr_key(ino);
            conn.del(&attr_key).await.map_err(|e| {
                slog::error!(self.logger, "Failed to delete file attributes"; "inode" => ino, "error" => ?e, "function" => "unlink");
                EIO
            })?;
        } else {
            // Update the nlink count in Redis
            let attr_key = metadata::inode_attr_key(ino);
            conn.hset(&attr_key, "nlink", attr.nlink).await.map_err(|e| {
                slog::error!(self.logger, "Failed to update nlink count"; "inode" => ino, "error" => ?e, "function" => "unlink");
                EIO
            })?;
        }

        // Remove file from parent directory
        conn.hdel(&parent_key, name_str).await.map_err(|e| {
            slog::error!(self.logger, "Failed to remove file from parent directory"; "parent_inode" => parent, "file_name" => ?name, "error" => ?e, "function" => "unlink");
            EIO
        })?;

        slog::debug!(self.logger, "Successfully unlinked file"; "parent_inode" => parent, "file_name" => ?name, "inode" => ino, "nlink" => attr.nlink, "function" => "unlink");
        Ok(())
    }

    pub async fn remove_file(&self, parent: u64, name: &OsStr, uid: u32, gid: u32) -> Result<(), i32> {
        // Directly call unlink function
        self.unlink(parent, name, uid, gid).await
    }

    pub async fn remove_directory(&self, parent: u64, name: &OsStr, uid: u32, gid: u32) -> Result<(), i32> {
        let parent_attr = self.get_attr(parent).await?;
        
        if !self.check_permission(&parent_attr, uid, gid, 2) {
            slog::warn!(self.logger, "Insufficient permissions to remove directory"; "parent_inode" => parent, "uid" => uid, "gid" => gid, "function" => "remove_directory");
            return Err(libc::EACCES);
        }

        let mut conn = self.redis_client.clone();
        let parent_key = metadata::inode_children_key(parent);
        let name_str = name.to_str().ok_or(libc::EINVAL)?;

        // Get directory's inode
        let ino: u64 = conn.hget(&parent_key, name_str).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get directory inode"; "parent_inode" => parent, "dir_name" => ?name, "error" => ?e, "function" => "remove_directory");
            EIO
        })?;

        // Check if directory is empty
        let dir_key = metadata::inode_children_key(ino);
        let children_count: u64 = conn.hlen(&dir_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get directory child count"; "inode" => ino, "error" => ?e, "function" => "remove_directory");
            EIO
        })?;

        if children_count > 2 {
            slog::warn!(self.logger, "Attempted to delete non-empty directory"; "inode" => ino, "child_count" => children_count, "function" => "remove_directory");
            return Err(ENOTEMPTY);
        } else if children_count == 2 {
            // If there are exactly 2 entries, check if they are "." and ".."
            let children: HashMap<String, String> = conn.hgetall(&dir_key).await.map_err(|e| {
                slog::error!(self.logger, "Failed to get directory children"; "inode" => ino, "error" => ?e, "function" => "remove_directory");
                EIO
            })?;

            if !children.keys().all(|k| k == "." || k == "..") {
                slog::warn!(self.logger, "Attempted to delete non-empty directory"; "inode" => ino, "child_count" => children_count, "function" => "remove_directory");
                return Err(ENOTEMPTY);
            }
        }

        // Delete directory attributes
        let attr_key = metadata::inode_attr_key(ino);
        conn.del(&attr_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to delete directory attributes"; "inode" => ino, "error" => ?e, "function" => "remove_directory");
            EIO
        })?;

        // Delete directory's children key
        conn.del(&dir_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to delete directory children key"; "inode" => ino, "error" => ?e, "function" => "remove_directory");
            EIO
        })?;

        // Remove directory from parent directory
        conn.hdel(&parent_key, name_str).await.map_err(|e| {
            slog::error!(self.logger, "Failed to remove directory from parent directory"; "parent_inode" => parent, "dir_name" => ?name, "error" => ?e, "function" => "remove_directory");
            EIO
        })?;

        slog::debug!(self.logger, "Successfully deleted directory"; "parent_inode" => parent, "dir_name" => ?name, "inode" => ino, "function" => "remove_directory");
        Ok(())
    }

    pub async fn link(&self, ino: u64, new_parent: u64, new_name: &OsStr, uid: u32, gid: u32) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();
        let new_parent_key = metadata::inode_children_key(new_parent);
        let new_name_str = new_name.to_str().ok_or(EINVAL)?;

        // Check permissions on the new parent directory
        let new_parent_attr = self.get_attr(new_parent).await?;
        if !self.check_permission(&new_parent_attr, uid, gid, 2) {  // 2 is for write permission
            slog::warn!(self.logger, "Insufficient permissions to create link in new parent directory"; "new_parent_inode" => new_parent, "uid" => uid, "gid" => gid, "function" => "link");
            return Err(libc::EACCES);
        }

        // Get the existing file's attributes
        let mut attr = self.get_attr(ino).await?;

        // Increase the nlink count
        attr.nlink += 1;
        let attr_opt = attr.into();
        self.set_attr(ino, &attr_opt).await?;

        // Add the new link to the parent directory
        conn.hset(&new_parent_key, new_name_str, ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new link to parent directory"; "parent_inode" => new_parent, "new_name" => ?new_name, "error" => ?e, "function" => "link");
            EIO
        })?;

        slog::debug!(self.logger, "Successfully created new link"; "inode" => ino, "new_parent" => new_parent, "new_name" => ?new_name, "function" => "link");
        Ok(attr)
    }
}

#[cfg(test)]
mod tests;
