use fuser::{Filesystem, Request, ReplyAttr, ReplyEntry, ReplyCreate, ReplyWrite, ReplyData, ReplyEmpty};
use tokio::task;

use crate::fs::redisfs::RedisFs;
use std::time::Duration;
use std::ffi::OsStr;
use libc::ENOENT;

use slog::{debug, warn, error};

impl Filesystem for RedisFs {
    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let fs = self.clone();
        let logger = self.logger.clone();
        
        task::spawn(async move {
            match fs.set_attr_opt(ino, mode, uid, gid, size, flags).await {
                Ok(attr) => {
                    debug!(logger, "Successfully set ino attributes"; 
                        "function" => "setattr",
                        "ino" => ino,
                        "attr" => ?attr
                    );
                    reply.attr(&Duration::new(0, 0), &attr);
                },
                Err(error_code) => {
                    error!(logger, "Failed to set ino attributes"; 
                        "function" => "setattr",
                        "ino" => ino,
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let fs = self.clone();
        let logger = self.logger.clone();
        
        task::spawn(async move {
            match fs.get_attr(ino).await {
                Ok(attr) => {
                    debug!(logger, "Successfully retrieved ino attributes"; 
                        "function" => "getattr",
                        "ino" => ino,
                        "attr" => ?attr
                    );
                    reply.attr(&Duration::new(0, 0), &attr);
                }
                Err(e) => {
                    error!(logger, "Failed to retrieve ino attributes"; 
                        "function" => "getattr",
                        "ino" => ino,
                        "error" => ?e
                    );
                    reply.error(ENOENT);
                }
            }
        });
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let fs = self.clone();
        let logger = self.logger.clone();
        
        let name = name.to_owned();
        task::spawn(async move {
            match fs.lookup(parent, &name).await {
                Ok(attr) => {
                    debug!(logger, "Successfully looked up file"; 
                        "function" => "lookup",
                        "parent" => parent,
                        "name" => ?name,
                        "attributes" => ?attr
                    );
                    reply.entry(&Duration::new(1, 0), &attr, 0)
                },
                Err(error_code) => {
                    warn!(logger, "Failed to look up file"; 
                        "function" => "lookup",
                        "parent" => parent,
                        "name" => ?name,
                        "error_code" => error_code
                    );
                    reply.error(error_code)
                },
            }
        });
    }

    fn create(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        let fs = self.clone();
        let logger = self.logger.clone();
        let name = name.to_owned();
        
        let req_uid = req.uid();
        let req_gid = req.gid();
        task::spawn(async move {
            match fs.create_file(parent, &name, mode, umask, flags, req_uid, req_gid).await {
                Ok((new_ino, attr)) => {
                    debug!(logger, "Successfully created file"; 
                        "function" => "create",
                        "parent_ino" => parent,
                        "file_name" => ?name,
                        "new_ino" => new_ino
                    );
                    reply.created(&Duration::new(1, 0), &attr, 0, 0, 0);
                },
                Err(error_code) => {
                    error!(logger, "Failed to create file"; 
                        "function" => "create",
                        "parent_ino" => parent,
                        "file_name" => ?name,
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn mkdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        let fs = self.clone();
        let logger = self.logger.clone();
        let name = name.to_owned();
        
        let req_uid = req.uid();
        let req_gid = req.gid();
        task::spawn(async move {
            match fs.create_directory(parent, &name, mode, umask, req_uid, req_gid).await {
                Ok((new_ino, attr)) => {
                    debug!(logger, "Successfully created directory"; 
                        "function" => "mkdir",
                        "parent_ino" => parent, 
                        "directory_name" => ?name, 
                        "new_ino" => new_ino
                    );
                    reply.entry(&Duration::new(1, 0), &attr, 0);
                },
                Err(error_code) => {
                    error!(logger, "Failed to create directory"; 
                        "function" => "mkdir",
                        "parent_ino" => parent, 
                        "directory_name" => ?name, 
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: fuser::ReplyDirectory) {
        let fs = self.clone();
        let logger = self.logger.clone();
        
        task::spawn(async move {
            debug!(logger, "Starting readdir"; "ino" => ino, "offset" => offset);
            match fs.read_dir(ino, offset).await {
                Ok(entries) => {
                    let entries_len = entries.len();
                    debug!(logger, "Retrieved entries"; "count" => entries_len);
                    for (i, (ino, name, attr)) in entries.into_iter().enumerate() {
                        let offset = (i + 1) as i64; // Offset starts from 1
                        let kind = attr.kind;
                        debug!(logger, "Adding entry to reply"; "ino" => ino, "name" => ?name, "kind" => ?kind);
                        let full = reply.add(ino, offset, kind, name);
                        if full {
                            debug!(logger, "Reply buffer full, breaking"; "last_ino" => ino);
                            break;
                        }
                    }
                    debug!(logger, "Successfully read directory"; 
                        "function" => "readdir",
                        "ino" => ino, 
                        "entries_count" => entries_len
                    );
                    reply.ok();
                },
                Err(error_code) => {
                    error!(logger, "Failed to read directory"; 
                        "function" => "readdir",
                        "ino" => ino, 
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn write(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let fs = self.clone();
        let logger = self.logger.clone();
        let data = data.to_vec(); // Create a copy of the data
        
        let req_uid = req.uid();
        let req_gid = req.gid();
        task::spawn(async move {
            match fs.write_file(ino, offset, &data, req_uid, req_gid).await {
                Ok(bytes_written) => {
                    debug!(logger, "Successfully wrote to file";
                        "function" => "write",
                        "ino" => ino,
                        "offset" => offset,
                        "bytes_written" => bytes_written
                    );
                    reply.written(bytes_written as u32);
                },
                Err(error_code) => {
                    error!(logger, "Failed to write to file";
                        "function" => "write",
                        "ino" => ino,
                        "offset" => offset,
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn read(
        &mut self,
        req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData
    ) {
        let fs = self.clone();
        let logger = self.logger.clone();
        
        let req_uid = req.uid();
        let req_gid = req.gid();
        task::spawn(async move {
            match fs.read_file(ino, offset, size, req_uid, req_gid).await {
                Ok(data) => {
                    debug!(logger, "Successfully read file";
                        "function" => "read",
                        "ino" => ino,
                        "offset" => offset,
                        "bytes_read" => data.len()
                    );
                    reply.data(&data);
                },
                Err(error_code) => {
                    error!(logger, "Failed to read file";
                        "function" => "read",
                        "ino" => ino,
                        "offset" => offset,
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn unlink(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let fs = self.clone();
        let logger = self.logger.clone();
        let name = name.to_owned();
        
        let req_uid = req.uid();
        let req_gid = req.gid();
        task::spawn(async move {
            match fs.remove_file(parent, &name, req_uid, req_gid).await {
                Ok(()) => {
                    slog::debug!(logger, "Successfully removed file";
                        "function" => "unlink",
                        "parent" => parent,
                        "name" => ?name
                    );
                    reply.ok();
                },
                Err(error_code) => {
                    slog::error!(logger, "Failed to remove file";
                        "function" => "unlink",
                        "parent" => parent,
                        "name" => ?name,
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }

    fn rmdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let fs = self.clone();
        let logger = self.logger.clone();
        let name = name.to_owned();
        
        let req_uid = req.uid();
        let req_gid = req.gid();
        task::spawn(async move {
            match fs.remove_directory(parent, &name, req_uid, req_gid).await {
                Ok(()) => {
                    slog::debug!(logger, "Successfully removed directory";
                        "function" => "rmdir",
                        "parent" => parent,
                        "name" => ?name
                    );
                    reply.ok();
                },
                Err(error_code) => {
                    slog::error!(logger, "Failed to remove directory";
                        "function" => "rmdir",
                        "parent" => parent,
                        "name" => ?name,
                        "error_code" => error_code
                    );
                    reply.error(error_code);
                }
            }
        });
    }
}