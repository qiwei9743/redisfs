use super::*;
use tokio;
use std::ffi::OsString;
use tokio::fs::remove_dir_all;

async fn create_test_redisfs() -> RedisFs {
    let redis_url = "redis://127.0.0.1/";
    let block_size = 4096;
    match RedisFs::new(redis_url, block_size).await {
        Ok(fs) => {
            println!("Successfully created RedisFs instance");
            fs
        },
        Err(e) => {
            eprintln!("Failed to create RedisFs: {:?}", e);
            panic!("Could not create RedisFs instance");
        }
    }
}

#[tokio::test]
async fn test_create_and_get_attr() {
    println!("Starting test_create_and_get_attr");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_file.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, attr) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 获取文件属性
    let attr2 = fs.get_attr(ino).await.expect("Failed to get file attributes");

    // 验证属性是否一致
    assert_eq!(attr.ino, attr2.ino);
    assert_eq!(attr.size, attr2.size);
    assert_eq!(attr.blocks, attr2.blocks);
    assert_eq!(attr.atime, attr2.atime);
    assert_eq!(attr.mtime, attr2.mtime);
    assert_eq!(attr.ctime, attr2.ctime);
    assert_eq!(attr.crtime, attr2.crtime);
    assert_eq!(attr.kind, attr2.kind);
    assert_eq!(attr.perm, attr2.perm);
    assert_eq!(attr.nlink, attr2.nlink);
    assert_eq!(attr.uid, attr2.uid);
    assert_eq!(attr.gid, attr2.gid);
    assert_eq!(attr.rdev, attr2.rdev);
    assert_eq!(attr.flags, attr2.flags);
    assert_eq!(attr.blksize, attr2.blksize);

    // 删���文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_read_file() {
    println!("Starting test_write_and_read_file");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_write_read.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入数据
    let data = b"Hello, World!";
    let bytes_written = fs.write_file_blocks(ino, 0, data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);

    // 读取数据
    let read_data = fs.read_file(ino, 0, data.len() as u32, uid, gid).await.expect("Failed to read file");
    assert_eq!(read_data, data);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_create_and_remove_directory() {
    println!("Starting test_create_and_remove_directory");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_dir");
    let mode = 0o755;
    let umask = 0o022;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建目录
    let (ino, attr) = fs.create_directory(parent, &name, mode, umask, uid, gid).await.expect("Failed to create directory");

    // 验证目录属性
    assert_eq!(attr.kind, FileType::Directory);
    assert_eq!(attr.perm, 0o755);

    // 删除目录
    fs.remove_directory(parent, &name, uid, gid).await.expect("Failed to remove directory");

    // 尝试获取已删除的目录属性,应该失败
    let result = fs.get_attr(ino).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_link_and_unlink() {
    println!("Starting test_link_and_unlink");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_link.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 创建硬链接
    let new_name = OsString::from("test_link_hard.txt");
    let attr = fs.link(ino, parent, &new_name, uid, gid).await.expect("Failed to create hard link");

    // 验证链接数增加
    assert_eq!(attr.nlink, 2);

    // 删除原始文件
    fs.unlink(parent, &name, uid, gid).await.expect("Failed to unlink original file");

    // 验证硬链接仍然存在
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.nlink, 1);

    // 删除硬链接
    fs.unlink(parent, &new_name, uid, gid).await.expect("Failed to unlink hard link");

    // 验证文件已完全删除
    let result = fs.get_attr(ino).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_read_dir() {
    println!("Starting test_read_dir");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录

    // 读取根目录内容
    let entries = fs.read_dir(parent, 0).await.expect("Failed to read directory");

    // 验证是否包含 "." 和 ".." 目录
    let mut has_dot = false;
    let mut has_dotdot = false;
    for (_, name, _) in &entries {
        if name == "." {
            has_dot = true;
        } else if name == ".." {
            has_dotdot = true;
        }
    }
    assert!(has_dot);
    assert!(has_dotdot);
}