use std::error::Error;
use std::fs::OpenOptions;
use redis;
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
use std::time::{SystemTime, Duration};
use fuser::FileType;

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

impl RedisFs {
    pub async fn new(redis_url: &str, block_size: u64) -> Result<Self, Box<dyn Error>> {
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

        let client = redis::Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;
        let redis_client = connection_manager;

        let fs = RedisFs { 
            redis_client,
            logger,
            block_size,
        };
        fs.ensure_root_inode().await?;
        Ok(fs)
    }

    async fn ensure_root_inode(&self) -> Result<(), Box<dyn Error>> {
        let mut conn = self.redis_client.clone();
        let root_key = "inode:1";
        
        if !conn.exists(root_key).await? {
            debug!(self.logger, "Root inode does not exist, creating"; "function" => "ensure_root_inode");
            
            let root_attr = vec![
                ("size", "0"),
                ("mode", "0755"),
                ("uid", "0"),
                ("gid", "0"),
                ("filetype", "3"),
            ];
            
            conn.hset_multiple(root_key, &root_attr).await?;
            conn.set("next_inode", 2).await?;
            
            debug!(self.logger, "Root inode created, next_inode set to 2"; "function" => "ensure_root_inode");
        } else {
            debug!(self.logger, "Root inode already exists"; "function" => "ensure_root_inode");
        }
        
        Ok(())
    }

    pub async fn set_attr(&self, ino: u64, attr: &FileAttr) -> Result<(), i32> {
        let mut conn = self.redis_client.clone();

        let attr_key = format!("inode:{}", ino);
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

        let mut pipe = redis::pipe();
        pipe.hset(&attr_key, "ino", ino)
            .hset(&attr_key, "size", attr.size)
            .hset(&attr_key, "mode", attr.perm)
            .hset(&attr_key, "uid", attr.uid)
            .hset(&attr_key, "gid", attr.gid)
            .hset(&attr_key, "atime", now)
            .hset(&attr_key, "mtime", now)
            .hset(&attr_key, "ctime", now)
            .hset(&attr_key, "crtime", now)
            .hset(&attr_key, "filetype", match attr.kind {
                fuser::FileType::Directory => 3,
                _ => 4,
            });

        pipe.query_async(&mut conn).await.map_err(|e| {
            slog::error!(self.logger, "Failed to set inode attributes"; "inode" => ino, "error" => ?e, "function" => "set_attr");
            EIO
        })?;

        Ok(())
    }

    pub async fn set_attr_opt(&self, ino: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, flags: Option<u32>) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();

        let attr_key = format!("inode:{}", ino);
        let mut pipe = redis::pipe();

        if let Some(mode) = mode {
            pipe.hset(&attr_key, "mode", mode);
        }
        if let Some(uid) = uid {
            pipe.hset(&attr_key, "uid", uid);
        }
        if let Some(gid) = gid {
            pipe.hset(&attr_key, "gid", gid);
        }
        if let Some(size) = size {
            pipe.hset(&attr_key, "size", size);
        }
        if let Some(flags) = flags {
            pipe.hset(&attr_key, "flags", flags);
        }

        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        pipe.hset(&attr_key, "ctime", now);

        pipe.query_async(&mut conn).await.map_err(|e| {
            slog::error!(self.logger, "Failed to set inode attributes"; "inode" => ino, "error" => ?e, "function" => "set_attr_opt");
            EIO
        })?;

        // Get the updated attributes
        self.get_attr(ino).await
    }

    pub async fn get_attr(&self, ino: u64) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();

        let attr_key = format!("inode:{}", ino);
        let attr: HashMap<String, String> = conn.hgetall(&attr_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get inode attributes"; "inode" => ino, "error" => ?e, "function" => "get_attr");
            EIO
        })?;

        if attr.is_empty() {
            slog::error!(self.logger, "Inode does not exist"; "inode" => ino, "function" => "get_attr");
            return Err(ENOENT);
        }

        let parse_time = |s: Option<&String>| {
            s.and_then(|t| t.parse::<u64>().ok())
                .map(|t| SystemTime::UNIX_EPOCH + Duration::from_secs(t))
                .unwrap_or_else(SystemTime::now)
        };

        let attr = FileAttr {
            ino,
            size: attr.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
            blocks: attr.get("blocks").and_then(|s| s.parse().ok()).unwrap_or(1),
            atime: parse_time(attr.get("atime")),
            mtime: parse_time(attr.get("mtime")),
            ctime: parse_time(attr.get("ctime")),
            crtime: parse_time(attr.get("crtime")),
            kind: match attr.get("kind").and_then(|t| t.parse::<u32>().ok()).unwrap_or(0) {
                1 => FileType::NamedPipe,
                2 => FileType::CharDevice,
                3 => FileType::BlockDevice,
                4 => FileType::Directory,
                5 => FileType::RegularFile,
                6 => FileType::Symlink,
                7 => FileType::Socket,
                _ => FileType::RegularFile,
            },
            perm: attr.get("perm").and_then(|m| m.parse().ok()).unwrap_or(0o644),
            nlink: attr.get("nlink").and_then(|n| n.parse().ok()).unwrap_or(1),
            uid: attr.get("uid").and_then(|u| u.parse().ok()).unwrap_or(0),
            gid: attr.get("gid").and_then(|g| g.parse().ok()).unwrap_or(0),
            rdev: attr.get("rdev").and_then(|r| r.parse().ok()).unwrap_or(0),
            flags: attr.get("flags").and_then(|f| f.parse().ok()).unwrap_or(0),
            blksize: self.block_size as u32,
        };

        slog::debug!(self.logger, "Successfully got inode attributes"; "inode" => ino, "attr" => ?attr, "function" => "get_attr");
        Ok(attr)
    }

    pub async fn lookup(&self, parent: u64, name: &OsStr) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();

        let parent_key = format!("inode:{}:children", parent);
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
        let file_key = format!("inode:{}:data", ino);

        let content: Vec<u8> = conn.get(&file_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to read file content"; "inode" => ino, "error" => ?e, "function" => "read_file");
            EIO
        })?;

        let file_size = content.len() as u64;
        slog::debug!(self.logger, "Starting to read file"; 
            "function" => "read_file",
            "inode" => ino, 
            "offset" => offset,
            "requested_size" => size,
            "file_size" => file_size
        );

        if offset as u64 >= file_size {
            return Ok(Vec::new());
        }

        let start = offset as usize;
        let end = std::cmp::min(start + size as usize, content.len());
        let data = content[start..end].to_vec();

        slog::debug!(self.logger, "Successfully read file"; "inode" => ino, "offset" => offset, "bytes_read" => data.len(), "function" => "read_file");
        Ok(data)
    }

    pub async fn write_file_blocks(&self, ino: u64, offset: i64, data: &[u8], uid: u32, gid: u32) -> Result<u64, i32> {
        let mut conn = self.redis_client.clone();
        let file_key = format!("inode:{}", ino);
        let attr: FileAttr = conn.hgetall(&file_key).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get inode attributes"; "inode" => ino, "error" => ?e, "function" => "write_file_blocks");
            EIO
        }).and_then(|data: HashMap<String, String>| {
            // Convert the HashMap to FileAttr
            use std::time::{SystemTime, Duration};
            use fuser::FileType;

            Ok(FileAttr {
                ino: ino,
                size: data.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
                blocks: data.get("blocks").and_then(|s| s.parse().ok()).unwrap_or(0),
                atime: SystemTime::UNIX_EPOCH + Duration::from_secs(data.get("atime").and_then(|s| s.parse().ok()).unwrap_or(0)),
                mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(data.get("mtime").and_then(|s| s.parse().ok()).unwrap_or(0)),
                ctime: SystemTime::UNIX_EPOCH + Duration::from_secs(data.get("ctime").and_then(|s| s.parse().ok()).unwrap_or(0)),
                crtime: SystemTime::UNIX_EPOCH + Duration::from_secs(data.get("crtime").and_then(|s| s.parse().ok()).unwrap_or(0)),
                kind: data.get("kind").and_then(|s| s.parse().ok()).map(|k: u32| match k {
                    1 => FileType::NamedPipe,
                    2 => FileType::CharDevice,
                    3 => FileType::BlockDevice,
                    4 => FileType::Directory,
                    5 => FileType::RegularFile,
                    6 => FileType::Symlink,
                    7 => FileType::Socket,
                    _ => FileType::RegularFile,
                }).unwrap_or(FileType::RegularFile),
                perm: data.get("perm").and_then(|s| s.parse().ok()).unwrap_or(0o644),
                nlink: data.get("nlink").and_then(|s| s.parse().ok()).unwrap_or(1),
                uid: data.get("uid").and_then(|s| s.parse().ok()).unwrap_or(0),
                gid: data.get("gid").and_then(|s| s.parse().ok()).unwrap_or(0),
                rdev: data.get("rdev").and_then(|s| s.parse().ok()).unwrap_or(0),
                flags: data.get("flags").and_then(|s| s.parse().ok()).unwrap_or(0),
                blksize: self.block_size as u32,
            })
        })?;

        if !self.check_permission(&attr, uid, gid, 2) {  // 2 is for write permission
            slog::warn!(self.logger, "Insufficient permissions to write file"; "inode" => ino, "uid" => uid, "gid" => gid, "function" => "write_file_blocks");
            return Err(libc::EACCES);
        }

        let start_block = (offset as u64) / self.block_size;
        let end_block = ((offset as u64 + data.len() as u64 - 1) / self.block_size).max(start_block);

        let mut bytes_written = 0;
        for block in start_block..=end_block {
            let block_offset = block * self.block_size;
            let data_offset = block_offset.saturating_sub(offset as u64) as usize;
            let block_start = offset.max(block_offset as i64) as usize - offset as usize;
            let block_end = ((block + 1) * self.block_size).min(offset as u64 + data.len() as u64) as usize - offset as usize;
            
            let block_data = &data[block_start..block_end];
            let block_key = format!("inode:{}:block:{}", ino, block);

            // Check if we need to read the existing block
            let need_read = data_offset != 0 || block_data.len() != self.block_size as usize;
            let block_exists = block_offset < attr.size;

            let mut existing_block = if need_read && block_exists {
                match conn.get::<_, Vec<u8>>(&block_key).await {
                    Ok(block_content) => block_content,
                    Err(_) => vec![0; self.block_size as usize],
                }
            } else {
                vec![0; self.block_size as usize]
            };

            if existing_block.len() < self.block_size as usize {
                existing_block.resize(self.block_size as usize, 0);
            }

            existing_block.splice(data_offset..data_offset+block_data.len(), block_data.iter().cloned());

            conn.set(&block_key, &existing_block).await.map_err(|e| {
                slog::error!(self.logger, "Failed to write file block"; "inode" => ino, "block" => block, "error" => ?e, "function" => "write_file_blocks");
                EIO
            })?;
            bytes_written += block_data.len() as u64;
        }

        let new_size = attr.size.max(offset as u64 + bytes_written);
        let new_mtime = std::time::SystemTime::now();
        conn.hset_multiple(&file_key, &[
            ("size", new_size.to_string()),
            ("mtime", new_mtime.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs().to_string()),
        ]).await.map_err(|e| {
            slog::error!(self.logger, "Failed to update inode attributes"; "inode" => ino, "error" => ?e, "function" => "write_file_blocks");
            EIO
        })?;

        Ok(bytes_written)
    }

    pub async fn create_file(&self, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, uid: u32, gid: u32) -> Result<(u64, FileAttr), i32> {
        let parent_attr = self.get_attr(parent).await?;
        
        if !self.check_permission(&parent_attr, uid, gid, 2) {  // 2 is for write permission in parent directory
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

        // Store the new file's attributes in Redis
        let attr_key = format!("inode:{}", new_ino);
        let mut pipe = redis::pipe();
        pipe.hset(&attr_key, "ino", new_ino)
            .hset(&attr_key, "size", 0)
            .hset(&attr_key, "mode", mode)
            .hset(&attr_key, "umask", umask)
            .hset(&attr_key, "flags", flags)
            .hset(&attr_key, "uid", uid)
            .hset(&attr_key, "gid", gid)
            .hset(&attr_key, "atime", now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs())
            .hset(&attr_key, "mtime", now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs())
            .hset(&attr_key, "ctime", now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs())
            .hset(&attr_key, "crtime", now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());

        pipe.query_async(&mut conn).await.map_err(|e| {
            error!(self.logger, "Failed to store new file attributes"; "inode" => new_ino, "error" => ?e, "function" => "create_file");
            EIO
        })?;

        // Add the new file to the parent directory
        let parent_key = format!("inode:{}:children", parent);
        conn.hset(&parent_key, name.to_str().unwrap(), new_ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new file to parent directory"; "parent_inode" => parent, "file_name" => ?name, "error" => ?e, "function" => "create_file");
            EIO
        })?;

        slog::info!(self.logger, "Successfully created new file"; "parent_inode" => parent, "file_name" => ?name, "new_inode" => new_ino, "function" => "create_file");
        Ok((new_ino, attr))
    }

    pub async fn create_directory(&self, parent: u64, name: &OsStr, mode: u32, umask: u32, uid: u32, gid: u32) -> Result<(u64, FileAttr), i32> {
        let parent_attr = self.get_attr(parent).await?;
        
        if !self.check_permission(&parent_attr, uid, gid, 2) {  // 2 is for write permission in parent directory
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
        let attr = FileAttr {
            ino: new_ino,
            size: 0,
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: fuser::FileType::Directory,
            perm: (mode & !umask) as u16,
            nlink: 2,  // Directories have 2 hard links by default: . and ..
            uid,
            gid,
            rdev: 0,
            flags: 0,
            blksize: self.block_size as u32,
        };

        // Set attributes for the new directory
        self.set_attr(new_ino, &attr).await.map_err(|e| {
            slog::error!(self.logger, "Failed to set new directory attributes"; "inode" => new_ino, "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        // Add the new directory to the parent directory
        let parent_key = format!("inode:{}:children", parent);
        conn.hset(&parent_key, name.to_str().unwrap(), new_ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new directory to parent"; "parent_inode" => parent, "dir_name" => ?name, "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        // Create . and .. entries for the new directory
        let children_key = format!("inode:{}:children", new_ino);
        let mut pipe = redis::pipe();
        pipe.hset(&children_key, ".", new_ino)
            .hset(&children_key, "..", parent);

        pipe.query_async(&mut conn).await.map_err(|e| {
            slog::error!(self.logger, "Failed to create . and .. entries for new directory"; "inode" => new_ino, "error" => ?e, "function" => "create_directory");
            EIO
        })?;

        slog::info!(self.logger, "Successfully created new directory"; "parent_inode" => parent, "dir_name" => ?name, "new_inode" => new_ino, "function" => "create_directory");
        Ok((new_ino, attr))
    }

    pub async fn read_dir(&self, parent: u64, offset: i64) -> Result<Vec<(u64, std::ffi::OsString, FileAttr)>, i32> {
        use std::ffi::OsString;

        let mut conn = self.redis_client.clone();

        let children_key = format!("inode:{}:children", parent);
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
        let parent_key = format!("inode:{}:children", parent);
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
            let file_key = format!("inode:{}:data", ino);
            conn.del(&file_key).await.map_err(|e| {
                slog::error!(self.logger, "Failed to delete file data"; "inode" => ino, "error" => ?e, "function" => "unlink");
                EIO
            })?;

            let attr_key = format!("inode:{}", ino);
            conn.del(&attr_key).await.map_err(|e| {
                slog::error!(self.logger, "Failed to delete file attributes"; "inode" => ino, "error" => ?e, "function" => "unlink");
                EIO
            })?;
        } else {
            // Update the nlink count in Redis
            let attr_key = format!("inode:{}", ino);
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
        
        if !self.check_permission(&parent_attr, uid, gid, 2) {  // 2 is for write permission in parent directory
            slog::warn!(self.logger, "Insufficient permissions to remove directory"; "parent_inode" => parent, "uid" => uid, "gid" => gid, "function" => "remove_directory");
            return Err(libc::EACCES);
        }

        let mut conn = self.redis_client.clone();
        let parent_key = format!("inode:{}:children", parent);
        let name_str = name.to_str().ok_or(libc::EINVAL)?;

        // Get directory's inode
        let ino: u64 = conn.hget(&parent_key, name_str).await.map_err(|e| {
            slog::error!(self.logger, "Failed to get directory inode"; "parent_inode" => parent, "dir_name" => ?name, "error" => ?e, "function" => "remove_directory");
            EIO
        })?;

        // Check if directory is empty
        let dir_key = format!("inode:{}:children", ino);
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
        let attr_key = format!("inode:{}:attr", ino);
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
        let new_parent_key = format!("inode:{}:children", new_parent);
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

        // Update the file's attributes in Redis
        let attr_key = format!("inode:{}", ino);
        conn.hset(&attr_key, "nlink", attr.nlink).await.map_err(|e| {
            slog::error!(self.logger, "Failed to update nlink count"; "inode" => ino, "error" => ?e, "function" => "link");
            EIO
        })?;

        // Add the new link to the parent directory
        conn.hset(&new_parent_key, new_name_str, ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new link to parent directory"; "parent_inode" => new_parent, "new_name" => ?new_name, "error" => ?e, "function" => "link");
            EIO
        })?;

        slog::debug!(self.logger, "Successfully created new link"; "inode" => ino, "new_parent" => new_parent, "new_name" => ?new_name, "function" => "link");
        Ok(attr)
    }
}

