use fuser::{Filesystem, Request, ReplyAttr, FileAttr, ReplyEntry, ReplyCreate, ReplyWrite, ReplyData};

use crate::fs::redisfs::RedisFs;
use std::time::Duration;
use std::ffi::OsStr;
use libc::ENOENT;

use slog::{debug, warn, error, Logger, o, Drain};


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
        match self.set_attr_opt(ino, mode, uid, gid, size, flags) {
            Ok(attr) => {
                slog::debug!(self.logger, "Successfully set ino attributes"; 
                    "function" => "setattr",
                    "ino" => ino,
                    "attr" => ?attr
                );
                reply.attr(&Duration::new(0, 0), &attr);
            },
            Err(error_code) => {
                slog::error!(self.logger, "Failed to set ino attributes"; 
                    "function" => "setattr",
                    "ino" => ino,
                    "error_code" => error_code
                );
                reply.error(error_code);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let attr = self.get_attr(ino);
        match attr {
            Ok(attr) => {
                slog::debug!(self.logger, "Successfully retrieved ino attributes"; 
                    "function" => "getattr",
                    "ino" => ino,
                    "attr" => ?attr
                );
                reply.attr(&Duration::new(0, 0), &attr);
            }
            Err(e) => {
                slog::error!(self.logger, "Failed to retrieve ino attributes"; 
                    "function" => "getattr",
                    "ino" => ino,
                    "error" => ?e
                );
                reply.error(ENOENT);
            }
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup(parent, name) {
            Ok(attr) => {
                slog::debug!(self.logger, "Successfully looked up file"; 
                    "function" => "lookup",
                    "parent" => parent,
                    "name" => ?name,
                    "attr" => ?attr
                );
                reply.entry(&Duration::new(1, 0), &attr, 0)
            },
            Err(error_code) => {
                slog::warn!(self.logger, "Failed to look up file"; 
                    "function" => "lookup",
                    "parent" => parent,
                    "name" => ?name,
                    "error_code" => error_code
                );
                reply.error(error_code)
            },
        }
    }
    fn create(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        match self.create_file(parent, name, mode, umask, flags, req.uid(), req.gid()) {
            Ok((new_ino, attr)) => {
                slog::debug!(self.logger, "Successfully created file"; 
                    "function" => "create",
                    "parent_ino" => parent,
                    "file_name" => ?name,
                    "new_ino" => new_ino
                );
                reply.created(&Duration::new(1, 0), &attr, 0, 0, 0);
            },
            Err(error_code) => {
                slog::error!(self.logger, "Failed to create file"; 
                    "function" => "create",
                    "parent_ino" => parent,
                    "file_name" => ?name,
                    "error_code" => error_code
                );
                reply.error(error_code);
            }
        }
    }
 
    fn mkdir(&mut self, req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, umask: u32, reply: ReplyEntry) {
        match self.create_directory(parent, name, mode, umask, req.uid(), req.gid()) {
            Ok((new_ino, attr)) => {
                slog::debug!(self.logger, "Successfully created directory"; 
                    "function" => "mkdir",
                    "parent_ino" => parent, 
                    "directory_name" => ?name, 
                    "new_ino" => new_ino
                );
                reply.entry(&Duration::new(1, 0), &attr, 0);
            },
            Err(error_code) => {
                slog::error!(self.logger, "Failed to create directory"; 
                    "function" => "mkdir",
                    "parent_ino" => parent, 
                    "directory_name" => ?name, 
                    "error_code" => error_code
                );
                reply.error(error_code);
            }
        }
    }
    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: fuser::ReplyDirectory) {
        match self.read_dir(ino, offset) {
            Ok(entries) => {
                let entries_len = entries.len();
                for (i, (ino, name, attr)) in entries.into_iter().enumerate() {
                    let offset = i as i64 + 1; // Offset starts from 1
                    let kind = match attr.kind {
                        fuser::FileType::Directory => fuser::FileType::Directory,
                        _ => fuser::FileType::RegularFile,
                    };
                    if reply.add(ino, offset, kind, name) {
                        break;
                    }
                }
                slog::debug!(self.logger, "Successfully read directory"; 
                    "function" => "readdir",
                    "ino" => ino, 
                    "entries_count" => entries_len
                );
                reply.ok();
            },
            Err(error_code) => {
                slog::error!(self.logger, "Failed to read directory"; 
                    "function" => "readdir",
                    "ino" => ino, 
                    "error_code" => ?error_code
                );
                reply.error(error_code);
            }
        }
    }
    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.write_file(ino, offset, data) {
            Ok(bytes_written) => {
                slog::debug!(self.logger, "Successfully wrote to file";
                    "function" => "write",
                    "ino" => ino,
                    "offset" => offset,
                    "bytes_written" => bytes_written
                );
                reply.written(bytes_written as u32);
            },
            Err(error_code) => {
                slog::error!(self.logger, "Failed to write to file";
                    "function" => "write",
                    "ino" => ino,
                    "offset" => offset,
                    "error_code" => error_code
                );
                reply.error(error_code);
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData
    ) {
        match self.read_file(ino, offset, size) {
            Ok(data) => {
                slog::debug!(self.logger, "Successfully read file";
                    "function" => "read",
                    "ino" => ino,
                    "offset" => offset,
                    "bytes_read" => data.len()
                );
                reply.data(&data);
            },
            Err(error_code) => {
                slog::error!(self.logger, "Failed to read file";
                    "function" => "read",
                    "ino" => ino,
                    "offset" => offset,
                    "error_code" => error_code
                );
                reply.error(error_code);
            }
        }
    }
}