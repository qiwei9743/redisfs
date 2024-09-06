use std::error::Error;
use std::fs::OpenOptions;
use redis;
use std::time::{Duration, UNIX_EPOCH};
use std::ffi::OsStr;
use libc::{ENOENT, EIO};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use tokio::sync::Mutex;
use slog::{debug, warn, error, Logger, o, Drain};
use slog_term;
use slog_async;
use std::io::{self, Write};
use snafu::{ResultExt, Whatever, Snafu};
use fuser::{FileAttr, FileType};
use slog_envlogger;
use std::sync::Arc;

pub struct RedisFs {
    redis_client: ConnectionManager,
    pub logger: Arc<Logger>,
}

impl Clone for RedisFs {
    fn clone(&self) -> Self {
        RedisFs {
            redis_client: self.redis_client.clone(),
            logger: Arc::clone(&self.logger),
        }
    }
}

impl RedisFs {
    pub async fn new(redis_url: &str) -> Result<Self, Box<dyn Error>> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("log.txt")?;

        // 创建一个终端风格的 Drain
        let decorator_term = slog_term::TermDecorator::new().build();
        let drain_term = slog_term::CompactFormat::new(decorator_term).build().fuse();
        let drain_term = slog_async::Async::new(drain_term).build().fuse();

        // 创建一个文件风格的 Drain
        let decorator_file = slog_term::PlainDecorator::new(file);
        let drain_file = slog_term::CompactFormat::new(decorator_file).build().fuse();
        let drain_file = slog_async::Async::new(drain_file).build().fuse();

        // 合并两个 Drain
        let drain = slog::Duplicate::new(drain_term, drain_file).fuse();
        
        // 使用 slog_envlogger 创建可配置的日志级别
        let drain = slog_envlogger::new(drain);
        
        // 创建根日志记录器
        let logger = Arc::new(slog::Logger::root(drain, o!("module" => "redisfs")));

        let client = redis::Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;
        let redis_client = connection_manager;

        let fs = RedisFs { 
            redis_client,
            logger,
        };
        fs.ensure_root_inode().await?;
        Ok(fs)
    }

    async fn ensure_root_inode(&self) -> Result<(), Box<dyn Error>> {
        let mut conn = self.redis_client.clone();
        let root_key = "inode:1";
        
        if !conn.exists(root_key).await? {
            debug!(self.logger, "根inode不存在，正在创建");
            
            let root_attr = vec![
                ("size", "0"),
                ("mode", "0755"),
                ("uid", "0"),
                ("gid", "0"),
                ("filetype", "3"),
            ];
            
            conn.hset_multiple(root_key, &root_attr).await?;
            conn.set("next_inode", 2).await?;
            
            debug!(self.logger, "根inode创建成功，next_inode设置为2");
        } else {
            debug!(self.logger, "根inode已存在");
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
            slog::error!(self.logger, "无法设置inode属性"; "inode" => ino, "错误" => ?e);
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
            slog::error!(self.logger, "无法设置inode属性"; "inode" => ino, "错误" => ?e, "函数" => "set_attr");
            EIO
        })?;

        // 获取更新后的属性
        self.get_attr(ino).await
    }

    pub async fn get_attr(&self, ino: u64) -> Result<FileAttr, i32> {
        let mut conn = self.redis_client.clone();

        let attr_key = format!("inode:{}", ino);
        let attr: std::collections::HashMap<String, String> = conn.hgetall(&attr_key).await.map_err(|e| {
            slog::error!(self.logger, "无法获取inode属性"; "inode" => ino, "错误" => ?e);
            EIO
        })?;

        if attr.is_empty() {
            slog::error!(self.logger, "inode不存在"; "inode" => ino);
            return Err(ENOENT);
        }

        let now = std::time::SystemTime::now();
        let attr = FileAttr {
            ino,
            size: attr.get("size").and_then(|s| s.parse().ok()).unwrap_or(0),
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: match attr.get("filetype").and_then(|t| t.parse::<u32>().ok()).unwrap_or(4) {
                3 => fuser::FileType::Directory,
                _ => fuser::FileType::RegularFile,
            },
            perm: attr.get("mode").and_then(|m| m.parse().ok()).unwrap_or(0o644),
            nlink: 1,
            uid: attr.get("uid").and_then(|u| u.parse().ok()).unwrap_or(0),
            gid: attr.get("gid").and_then(|g| g.parse().ok()).unwrap_or(0),
            rdev: 0,
            flags: 0,
            blksize: 4096,
        };

        slog::debug!(self.logger, "成功获取inode属性"; "inode" => ino, "attr" => ?attr);
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
                        error!(self.logger, "Failed to get attributes"; "inode" => ino, "error" => ?e);
                        Err(EIO)
                    }
                }
            },
            Err(_) => Err(ENOENT),
        }
    }

    pub async fn create_file(&self, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, uid: u32, gid: u32) -> Result<(u64, FileAttr), i32> {
        let mut conn = self.redis_client.clone();

        // Generate new inode number
        let new_ino: u64 = conn.incr("next_inode", 1).await.map_err(|e| {
            error!(self.logger, "Failed to generate new inode number"; "error" => ?e);
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
            blksize: 4096,
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
            error!(self.logger, "Failed to store new file attributes"; "inode" => new_ino, "error" => ?e);
            EIO
        })?;

        // Add the new file to the parent directory
        let parent_key = format!("inode:{}:children", parent);
        conn.hset(&parent_key, name.to_str().unwrap(), new_ino).await.map_err(|e| {
            slog::error!(self.logger, "Failed to add new file to parent directory"; "parent_inode" => parent, "file_name" => ?name, "error" => ?e);
            EIO
        })?;

        slog::info!(self.logger, "Successfully created new file"; "parent_inode" => parent, "file_name" => ?name, "new_inode" => new_ino);
        Ok((new_ino, attr))
    }

    pub async fn create_directory(&self, parent: u64, name: &OsStr, mode: u32, umask: u32, uid: u32, gid: u32) -> Result<(u64, FileAttr), i32> {
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
            blksize: 4096,
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

        slog::debug!(self.logger, "Successfully read directory contents"; "parent_inode" => parent, "entry_count" => entries.len(), "offset" => offset);
        Ok(entries)
    }

    pub async fn write_file(&self, ino: u64, offset: i64, data: &[u8]) -> Result<u32, i32> {
        let mut conn = self.redis_client.clone();

        let file_key = format!("inode:{}:data", ino);
        let file_size: u64 = conn.get(&file_key).await.unwrap_or(0);

        let new_size = std::cmp::max(file_size, (offset as u64) + (data.len() as u64));
        let mut file_content = vec![0u8; new_size as usize];

        if file_size > 0 {
            let existing_content: Vec<u8> = conn.get(&file_key).await.map_err(|e| {
                slog::error!(self.logger, "读取文件内容失败"; "inode" => ino, "错误" => ?e, "函数" => "write_file");
                EIO
            })?;
            file_content[..file_size as usize].copy_from_slice(&existing_content);
        }

        file_content[offset as usize..offset as usize + data.len()].copy_from_slice(data);

        conn.set(&file_key, &file_content).await.map_err(|e| {
            slog::error!(self.logger, "写入文件内容失败"; "inode" => ino, "错误" => ?e, "函数" => "write_file");
            EIO
        })?;

        // 更新文件大小
        self.set_attr_opt(ino, None, None, None, Some(new_size), None).await?;

        slog::debug!(self.logger, "成功写入文件"; "inode" => ino, "偏移量" => offset, "写入字节数" => data.len());
        Ok(data.len() as u32)
    }

    pub async fn read_file(&self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>, i32> {
        let mut conn = self.redis_client.clone();

        let file_key = format!("inode:{}:data", ino);

        let content: Vec<u8> = conn.get(&file_key).await.map_err(|e| {
            slog::error!(self.logger, "读取文件内容失败"; "inode" => ino, "错误" => ?e, "函数" => "read_file");
            EIO
        })?;

        let file_size = content.len() as u64;
        slog::debug!(self.logger, "开始读取文件"; 
            "函数" => "read_file",
            "inode" => ino, 
            "偏移量" => offset,
            "请求大小" => size,
            "文件大小" => file_size
        );

        if offset as u64 >= file_size {
            return Ok(Vec::new());
        }

        let start = offset as usize;
        let end = std::cmp::min(start + size as usize, content.len());
        let data = content[start..end].to_vec();

        slog::debug!(self.logger, "成功读取文件"; "inode" => ino, "偏移量" => offset, "读取字节数" => data.len());
        Ok(data)
    }
}
