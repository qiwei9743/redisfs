use std::error::Error;
use std::collections::{HashMap, HashSet};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use slog::{Logger, o, Drain};
use std::sync::Arc;

pub struct RedisFsck {
    redis_client: ConnectionManager,
    logger: Arc<Logger>,
}

impl RedisFsck {
    pub async fn new(redis_url: &str) -> Result<Self, Box<dyn Error>> {
        // 创建日志记录器
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open("fsck_log.txt")?;

        // 创建一个终端风格的 Drain
        let decorator_term = slog_term::TermDecorator::new().build();
        let drain_term = slog_term::FullFormat::new(decorator_term).build().fuse();
        let drain_term = slog_async::Async::new(drain_term).build().fuse();

        // 创建一个文件风格的 Drain
        let decorator_file = slog_term::PlainDecorator::new(file);
        let drain_file = slog_term::FullFormat::new(decorator_file).build().fuse();
        let drain_file = slog_async::Async::new(drain_file).build().fuse();

        // 合并两个 Drain
        let drain = slog::Duplicate::new(drain_term, drain_file).fuse();
        
        // 使用 slog_envlogger 创建可配置的日志级别
        let drain = slog_envlogger::new(drain).fuse();
        
        // 创建根日志记录器
        let logger = Arc::new(Logger::root(drain, o!("module" => "fsck")));

        let client = redis::Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;
        
        Ok(RedisFsck {
            redis_client: connection_manager,
            logger,
        })
    }

    pub async fn check_and_repair(&self) -> Result<(), Box<dyn Error>> {
        slog::info!(self.logger, "Starting file system check and repair");
        
        self.check_inodes().await?;
        self.check_directory_structure().await?;
        self.check_file_data().await?;
        self.repair_inconsistencies().await?;
        
        slog::info!(self.logger, "File system check and repair completed");
        Ok(())
    }

    async fn check_inodes(&self) -> Result<(), Box<dyn Error>> {
        slog::info!(self.logger, "Checking inodes");
        let mut conn = self.redis_client.clone();

        let keys: Vec<String> = conn.keys("inode:*").await?;
        slog::info!(self.logger, "Retrieved {} inode keys from Redis", keys.len());
        
        for key in keys {
            let inode: u64 = key.split(':').nth(1).unwrap().parse()?;
            let attrs: HashMap<String, String> = conn.hgetall(&key).await?;
            slog::info!(self.logger, "Checking inode"; "inode" => inode, "attributes" => ?attrs);

            let required_attrs = vec!["size", "mode", "uid", "gid", "filetype"];
            for attr in required_attrs {
                if !attrs.contains_key(attr) {
                    slog::warn!(self.logger, "Inode is missing required attribute"; "inode" => inode, "missing_attr" => attr);
                    // TODO: Implement attribute repair
                    slog::warn!(self.logger, "Attribute repair not implemented yet"; "inode" => inode, "attribute" => attr);
                }
            }

            if let Some(filetype) = attrs.get("filetype") {
                let filetype: u32 = filetype.parse()?;
                if filetype != 3 && filetype != 4 {
                    slog::warn!(self.logger, "Inode has invalid file type"; "inode" => inode, "filetype" => filetype);
                    // TODO: Implement file type repair
                    slog::warn!(self.logger, "File type repair not implemented yet"; "inode" => inode);
                }
            }
        }

        Ok(())
    }

    async fn check_directory_structure(&self) -> Result<(), Box<dyn Error>> {
        slog::info!(self.logger, "Checking directory structure");
        let mut conn = self.redis_client.clone();

        let dir_keys: Vec<String> = conn.keys("inode:*:children").await?;
        slog::info!(self.logger, "Retrieved {} directory keys from Redis", dir_keys.len());
        
        for dir_key in dir_keys {
            let inode: u64 = dir_key.split(':').nth(1).unwrap().parse()?;
            let children: HashMap<String, String> = conn.hgetall(&dir_key).await?;
            slog::info!(self.logger, "Checking directory structure"; "inode" => inode, "child_count" => children.len());

            if !children.contains_key(".") || !children.contains_key("..") {
                slog::warn!(self.logger, "Directory is missing '.' or '..' entries"; "inode" => inode);
                // TODO: Implement repair for missing . and .. entries
                slog::warn!(self.logger, "Repair for missing '.' and '..' entries not implemented yet"; "inode" => inode);
            }

            for (name, child_inode) in children {
                if name != "." && name != ".." {
                    let child_key = format!("inode:{}", &child_inode);
                    let exists: bool = conn.exists(&child_key).await?;
                    slog::info!(self.logger, "Checking child inode"; "parent_inode" => inode, "child_name" => &name, "child_inode" => &child_inode, "exists" => exists);
                    if !exists {
                        slog::warn!(self.logger, "Directory contains non-existent child"; "parent_inode" => inode, "child_name" => &name, "child_inode" => child_inode);
                        // TODO: Implement removal of non-existent child
                        slog::warn!(self.logger, "Removal of non-existent child not implemented yet"; "parent_inode" => inode, "child_name" => name);
                    }
                }
            }
        }

        Ok(())
    }

    async fn check_file_data(&self) -> Result<(), Box<dyn Error>> {
        slog::info!(self.logger, "Checking file data");
        let mut conn = self.redis_client.clone();

        let file_keys: Vec<String> = conn.keys("inode:*").await?;
        slog::info!(self.logger, "Retrieved {} inode keys from Redis", file_keys.len());
        
        for file_key in file_keys {
            let inode: u64 = file_key.split(':').nth(1).unwrap().parse()?;
            let attrs: HashMap<String, String> = conn.hgetall(&file_key).await?;
            slog::info!(self.logger, "Checking file data"; "inode" => inode, "attributes" => ?attrs);

            if attrs.get("filetype").unwrap() == "4" {  // Regular file
                let data_key = format!("inode:{}:data", inode);
                let size: u64 = attrs.get("size").unwrap().parse()?;
                let actual_size: u64 = conn.strlen(&data_key).await?;
                slog::info!(self.logger, "Comparing file sizes"; "inode" => inode, "expected_size" => size, "actual_size" => actual_size);

                if size != actual_size {
                    slog::warn!(self.logger, "File size mismatch"; "inode" => inode, "expected_size" => size, "actual_size" => actual_size);
                    // TODO: Implement file size mismatch repair
                    slog::warn!(self.logger, "File size mismatch repair not implemented yet"; "inode" => inode);
                }
            }
        }

        Ok(())
    }

    async fn repair_inconsistencies(&self) -> Result<(), Box<dyn Error>> {
        slog::info!(self.logger, "Repairing inconsistencies");
        let mut conn = self.redis_client.clone();

        let all_inodes: HashSet<u64> = conn.keys::<&str, Vec<String>>("inode:*")
            .await?
            .into_iter()
            .filter_map(|key| key.split(':').nth(1)?.parse().ok())
            .collect();
        slog::info!(self.logger, "Retrieved {} total inodes", all_inodes.len());

        let mut referenced_inodes = HashSet::new();
        let dir_keys: Vec<String> = conn.keys("inode:*:children").await?;
        slog::info!(self.logger, "Retrieved {} directory keys", dir_keys.len());
        
        for dir_key in dir_keys {
            let children: HashMap<String, String> = conn.hgetall(&dir_key).await?;
            for child_inode in children.values() {
                referenced_inodes.insert(child_inode.parse::<u64>()?);
            }
        }
        slog::info!(self.logger, "Found {} referenced inodes", referenced_inodes.len());

        let orphaned_inodes: Vec<u64> = all_inodes.difference(&referenced_inodes).cloned().collect();
        slog::info!(self.logger, "Found {} orphaned inodes", orphaned_inodes.len());

        for orphaned_inode in orphaned_inodes {
            slog::warn!(self.logger, "Found orphaned inode"; "inode" => orphaned_inode);
            // TODO: Implement orphaned inode handling
            slog::warn!(self.logger, "Orphaned inode handling not implemented yet"; "inode" => orphaned_inode);
        }

        Ok(())
    }

    pub async fn check_consistency_from_root(&self) -> Result<(), Box<dyn Error>> {
        slog::info!(self.logger, "Starting consistency check from root inode");
        let mut conn = self.redis_client.clone();

        let root_inode = 1; // Assume root inode is always 1
        let mut visited_inodes = HashSet::new();
        let mut stack = vec![root_inode];

        while let Some(inode) = stack.pop() {
            if visited_inodes.contains(&inode) {
                continue;
            }
            visited_inodes.insert(inode);
            slog::info!(self.logger, "Checking inode"; "inode" => %inode);

            let inode_key = format!("inode:{}", inode);
            let attrs: HashMap<String, String> = conn.hgetall(&inode_key).await?;

            if attrs.is_empty() {
                slog::warn!(self.logger, "Inode does not exist"; "inode" => %inode);
                continue;
            }

            // Check inode attributes
            let required_attrs = vec!["size", "mode", "uid", "gid", "filetype"];
            for attr in required_attrs {
                if !attrs.contains_key(attr) {
                    slog::warn!(self.logger, "Inode is missing required attribute"; "inode" => %inode, "missing_attr" => %attr);
                }
            }

            // Check file type
            let filetype = attrs.get("filetype").and_then(|ft| ft.parse::<u32>().ok()).unwrap_or(0);
            if filetype != 3 && filetype != 4 {
                slog::warn!(self.logger, "Inode has invalid file type"; "inode" => %inode, "filetype" => %filetype);
            }

            // If it's a directory, check its children
            if filetype == 3 {
                let children_key = format!("inode:{}:children", inode);
                let children: HashMap<String, String> = conn.hgetall(&children_key).await?;

                if !children.contains_key(".") || !children.contains_key("..") {
                    slog::warn!(self.logger, "Directory is missing '.' or '..' entries"; "inode" => %inode);
                }

                for (name, child_inode) in children {
                    if name != "." && name != ".." {
                        if let Ok(child_inode) = child_inode.parse::<u64>() {
                            stack.push(child_inode);
                        }
                    }
                }
            } else if filetype == 4 {
                // If it's a regular file, check its data
                let data_key = format!("inode:{}:data", inode);
                let size: u64 = attrs.get("size").and_then(|s| s.parse().ok()).unwrap_or(0);
                let actual_size: u64 = conn.strlen(&data_key).await?;

                if size != actual_size {
                    slog::warn!(self.logger, "File size mismatch"; "inode" => inode, "expected_size" => size, "actual_size" => actual_size);
                }
            }
        }

        // Check for orphaned inodes
        let mut all_inodes = HashSet::new();
        let mut scan = conn.scan_match::<&str, String>("inode:*").await?;
        while let Some(key) = scan.next_item().await {
            if let Some(inode_str) = key.split(':').nth(1) {
                if let Ok(inode) = inode_str.parse() {
                    all_inodes.insert(inode);
                }
            }
        }

        let orphaned_inodes: Vec<u64> = all_inodes.difference(&visited_inodes).cloned().collect();
        slog::info!(self.logger, "Found {} orphaned inodes", orphaned_inodes.len());

        for orphaned_inode in orphaned_inodes {
            slog::warn!(self.logger, "Found orphaned inode"; "inode" => orphaned_inode);
            // TODO: Implement orphaned inode handling
            slog::warn!(self.logger, "Orphaned inode handling not implemented yet"; "inode" => orphaned_inode);
        }

        slog::info!(self.logger, "Consistency check from root completed");
        Ok(())
    }
}
