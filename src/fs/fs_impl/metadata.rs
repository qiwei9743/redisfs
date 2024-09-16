use std::time::SystemTime;
use fuser::{FileAttr, FileType};
use std::collections::HashMap;
use std::time::{UNIX_EPOCH, Duration};


/// 序列化 SystemTime 为字符串，精确到纳秒
fn serialize_system_time(time: SystemTime) -> String {
    let duration_since_epoch = time.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let seconds = duration_since_epoch.as_secs();
    let nanos = duration_since_epoch.subsec_nanos();
    format!("{}.{}", seconds, nanos)  // 秒和纳秒用点号分隔
}

/// 从字符串反序列化回 SystemTime
fn deserialize_system_time(s: &str) -> Option<SystemTime> {
    let parts: Vec<&str> = s.split('.').collect();  // 将秒和纳秒部分拆分
    let seconds: u64 = parts[0].parse().ok()?;
    let nanos: u32 = parts[1].parse().ok()?;

    Some(SystemTime::UNIX_EPOCH + Duration::new(seconds, nanos))
}

#[derive(Debug, Default)]
pub struct FileOptionAttr {
    pub ino: Option<u64>,
    pub size: Option<u64>,
    pub blocks: Option<u64>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
    pub ctime: Option<SystemTime>,
    pub crtime: Option<SystemTime>,
    pub kind: Option<FileType>,
    pub perm: Option<u16>,
    pub nlink: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub rdev: Option<u32>,
    pub blksize: Option<u32>,
    pub flags: Option<u32>,
}

impl From<FileOptionAttr> for FileAttr {
    fn from(file_option_attr: FileOptionAttr) -> Self {
        FileAttr {
            ino: file_option_attr.ino.unwrap_or_default(),
            size: file_option_attr.size.unwrap_or_default(),
            blocks: file_option_attr.blocks.unwrap_or_default(),
            atime: file_option_attr.atime.unwrap_or(SystemTime::UNIX_EPOCH),
            mtime: file_option_attr.mtime.unwrap_or(SystemTime::UNIX_EPOCH),
            ctime: file_option_attr.ctime.unwrap_or(SystemTime::UNIX_EPOCH),
            crtime: file_option_attr.crtime.unwrap_or(SystemTime::UNIX_EPOCH),
            kind: file_option_attr.kind.unwrap_or(FileType::RegularFile),
            perm: file_option_attr.perm.unwrap_or_default(),
            nlink: file_option_attr.nlink.unwrap_or_default(),
            uid: file_option_attr.uid.unwrap_or_default(),
            gid: file_option_attr.gid.unwrap_or_default(),
            rdev: file_option_attr.rdev.unwrap_or_default(),
            blksize: file_option_attr.blksize.unwrap_or_default(),
            flags: file_option_attr.flags.unwrap_or_default(),
        }
    }
}


impl From<FileAttr> for FileOptionAttr {
    fn from(file_attr: FileAttr) -> Self {
        FileOptionAttr {
            ino: Some(file_attr.ino),
            size: Some(file_attr.size),
            blocks: Some(file_attr.blocks),
            atime: Some(file_attr.atime),
            mtime: Some(file_attr.mtime),
            ctime: Some(file_attr.ctime),
            crtime: Some(file_attr.crtime),
            kind: Some(file_attr.kind),
            perm: Some(file_attr.perm),
            nlink: Some(file_attr.nlink),
            uid: Some(file_attr.uid),
            gid: Some(file_attr.gid),
            rdev: Some(file_attr.rdev),
            blksize: Some(file_attr.blksize),
            flags: Some(file_attr.flags),
        }
    }
}

impl From<HashMap<String, String>> for FileOptionAttr {
    fn from(map: HashMap<String, String>) -> Self {
        FileOptionAttr {
            ino: map.get("ino").and_then(|v| v.parse().ok()),
            size: map.get("size").and_then(|v| v.parse().ok()),
            blocks: map.get("blocks").and_then(|v| v.parse().ok()),
            atime: map.get("atime").and_then(|v| deserialize_system_time(v.as_str())),
            mtime: map.get("mtime").and_then(|v| deserialize_system_time(v.as_str())),
            ctime: map.get("ctime").and_then(|v| deserialize_system_time(v.as_str())),
            crtime: map.get("crtime").and_then(|v| deserialize_system_time(v.as_str())),
            kind: map.get("kind").and_then(|v| v.parse().ok().map(|k| match k {
                0 => FileType::NamedPipe,
                1 => FileType::CharDevice,
                2 => FileType::BlockDevice,
                3 => FileType::Directory,
                4 => FileType::RegularFile,
                5 => FileType::Symlink,
                6 => FileType::Socket,
                _ => FileType::RegularFile,
            })),
            perm: map.get("perm").and_then(|v| v.parse().ok()),
            nlink: map.get("nlink").and_then(|v| v.parse().ok()),
            uid: map.get("uid").and_then(|v| v.parse().ok()),
            gid: map.get("gid").and_then(|v| v.parse().ok()),
            rdev: map.get("rdev").and_then(|v| v.parse().ok()),
            blksize: map.get("blksize").and_then(|v| v.parse().ok()),
            flags: map.get("flags").and_then(|v| v.parse().ok()),
        }

        
    }
}


impl FileOptionAttr {
    // Convert FileOptionAttr to HashMap<String, String>
    // TODO(qiwei): optimize string into small bytes.
    // for key, 1 for inode. 2 for size, 3 for blocks, 4 for atime, 5 for mtime, 6 for ctime, 7 for crtime, 8 for kind, 9 for perm, 10 for nlink, 11 for uid, 12 for gid, 13 for rdev, 14 for blksize, 15 for flags
    // for value, variant int is good.
    pub fn to_redis_hash(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(ino) = self.ino {
            map.insert("ino".to_string(), ino.to_string());
        }
        if let Some(size) = self.size {
            map.insert("size".to_string(), size.to_string());
        }
        if let Some(blocks) = self.blocks {
            map.insert("blocks".to_string(), blocks.to_string());
        }
        if let Some(atime) = self.atime {
            map.insert("atime".to_string(), serialize_system_time(atime));
        }
        if let Some(mtime) = self.mtime {
            map.insert("mtime".to_string(), serialize_system_time(mtime));
        }
        if let Some(ctime) = self.ctime {
            map.insert("ctime".to_string(), serialize_system_time(ctime));
        }
        if let Some(crtime) = self.crtime {
            map.insert("crtime".to_string(), serialize_system_time(crtime));
        }
        if let Some(kind) = self.kind {
            map.insert("kind".to_string(), (kind as i64).to_string());
        }
        if let Some(perm) = self.perm {
            map.insert("perm".to_string(), perm.to_string());
        }
        if let Some(nlink) = self.nlink {
            map.insert("nlink".to_string(), nlink.to_string());
        }
        if let Some(uid) = self.uid {
            map.insert("uid".to_string(), uid.to_string());
        }
        if let Some(gid) = self.gid {
            map.insert("gid".to_string(), gid.to_string());
        }
        if let Some(rdev) = self.rdev {
            map.insert("rdev".to_string(), rdev.to_string());
        }
        if let Some(blksize) = self.blksize {
            map.insert("blksize".to_string(), blksize.to_string());
        }
        if let Some(flags) = self.flags {
            map.insert("flags".to_string(), flags.to_string());
        }
        map
    }

}


pub fn inode_attr_key(ino: u64) -> String {
    format!("inode:{}::attr", ino)
}
pub fn inode_children_key(ino: u64) -> String {
    format!("inode:{}::children", ino)
}
pub fn inode_block_key(ino: u64, block_id: u64) -> String {
    format!("inode:{}::block::{}", ino, block_id)
}
pub fn inode_data_key(ino: u64) -> String {
    format!("inode:{}::data", ino)
}
