use super::*;
use tokio;
use std::ffi::OsString;
use redis::AsyncCommands;
use rand::Rng;
use std::sync::Arc;

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

    // 删除文件
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

#[tokio::test]
async fn test_create_and_get_attr_with_different_modes() {
    println!("Starting test_create_and_get_attr_with_different_modes");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_file_mode.txt");
    let modes = [0o644, 0o600, 0o755];
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    for &mode in &modes {
        // 创建文件
        let (ino, attr) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

        // 获取文件属性
        let attr2 = fs.get_attr(ino).await.expect("Failed to get file attributes");

        // 验证属性是否一致
        assert_eq!(attr.perm, mode as u16);
        assert_eq!(attr.ino, attr2.ino);

        // 删除文件
        fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
    }
}

#[tokio::test]
async fn test_write_and_read_large_file() {
    println!("Starting test_write_and_read_large_file");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_large_file.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入大数据
    let data = vec![0u8; 10 * 1024 * 1024]; // 10MB
    let bytes_written = fs.write_file_blocks(ino, 0, &data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);

    // 读取数据
    let read_data = fs.read_file(ino, 0, data.len() as u32, uid, gid).await.expect("Failed to read file");
    assert_eq!(read_data, data);

    // 验证文件大小和块数
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, data.len() as u64, "File size incorrect");
    let expected_blocks = (data.len() as u64 + 4095) / 4096; // 向上取整到4096字块
    assert_eq!(attr.blocks, expected_blocks, "Block count incorrect");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_create_and_remove_nested_directories() {
    println!("Starting test_create_and_remove_nested_directories");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let dir1 = OsString::from("dir1");
    let dir2 = OsString::from("dir1/dir2");
    let mode = 0o755;
    let umask = 0o022;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建第一级目录
    let (ino1, attr1) = fs.create_directory(parent, &dir1, mode, umask, uid, gid).await.expect("Failed to create directory");
    assert_eq!(attr1.kind, FileType::Directory);

    // 创建第二级目录
    let (ino2, attr2) = fs.create_directory(ino1, &dir2, mode, umask, uid, gid).await.expect("Failed to create directory");
    assert_eq!(attr2.kind, FileType::Directory);

    // 删除第二级目录
    fs.remove_directory(ino1, &dir2, uid, gid).await.expect("Failed to remove directory");

    // 删除第一级目录
    fs.remove_directory(parent, &dir1, uid, gid).await.expect("Failed to remove directory");
}

#[tokio::test]
async fn test_link_and_unlink_multiple_links() {
    println!("Starting test_link_and_unlink_multiple_links");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_multi_link.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 创建多个硬链接
    let link_names = ["link1.txt", "link2.txt", "link3.txt"];
    for link_name in &link_names {
        let link_name = OsString::from(link_name);
        let attr = fs.link(ino, parent, &link_name, uid, gid).await.expect("Failed to create hard link");
        assert_eq!(attr.nlink, 2 + link_names.iter().position(|&x| x == link_name.to_str().unwrap()).unwrap() as u32);
    }

    // 删除所有硬链接
    for link_name in &link_names {
        let link_name = OsString::from(link_name);
        fs.unlink(parent, &link_name, uid, gid).await.expect("Failed to unlink hard link");
    }
    
    fs.unlink(parent, &name, uid, gid).await.expect("Failed to unlink original file");
    // 验证文件已完全删除
    let result = fs.get_attr(ino).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_read_dir_with_files() {
    println!("Starting test_read_dir_with_files");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let file_names = ["file1.txt", "file2.txt", "file3.txt"];
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建多个文件
    for file_name in &file_names {
        let file_name = OsString::from(file_name);
        fs.create_file(parent, &file_name, mode, umask, flags, uid, gid).await.expect("Failed to create file");
    }

    // 读取根目录内容
    let entries = fs.read_dir(parent, 0).await.expect("Failed to read directory");

    // 验证是否包含创建的文件
    for file_name in &file_names {
        let mut found = false;
        for (_, name, _) in &entries {
            if name == file_name {
                found = true;
                break;
            }
        }
        assert!(found, "Directory does not contain expected file: {}", file_name);
    }

    // 删除文件
    for file_name in &file_names {
        let file_name = OsString::from(file_name);
        fs.remove_file(parent, &file_name, uid, gid).await.expect("Failed to remove file");
    }
}

#[tokio::test]
async fn test_read_dir_with_files2() {
    println!("Starting test_read_dir_with_files");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let file_names = ["file1.txt", "file2.txt", "file3.txt"];
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建多个文件
    for file_name in &file_names {
        let file_name = OsString::from(file_name);
        fs.create_file(parent, &file_name, mode, umask, flags, uid, gid).await.expect("Failed to create file");
    }

    // 读取根目录内容
    let entries = fs.read_dir(parent, 0).await.expect("Failed to read directory");

    // 验证是否包含创建的文件
    for file_name in &file_names {
        let mut found = false;
        for (_, name, _) in &entries {
            if name == file_name {
                found = true;
                break;
            }
        }
        assert!(found, "Directory does not contain expected file: {}", file_name);
    }

    // 删除文件
    for file_name in &file_names {
        let file_name = OsString::from(file_name);
        fs.remove_file(parent, &file_name, uid, gid).await.expect("Failed to remove file");
    }
}

#[tokio::test]
async fn test_write_and_read_non_aligned() {
    println!("Starting test_write_and_read_non_aligned");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_non_aligned.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入非4K对齐的数据
    let data = b"Hello, World! This is a test for non-aligned write.";
    let bytes_written = fs.write_file_blocks(ino, 0, data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);

    // 读取数据
    let read_data = fs.read_file(ino, 0, data.len() as u32, uid, gid).await.expect("Failed to read file");
    assert_eq!(read_data, data);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_read_boundary_conditions() {
    println!("Starting test_write_and_read_boundary_conditions");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_boundary_conditions.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入数据到文件末尾
    let data = b"Boundary test data";
    let offset = 4096 - data.len() as i64; // 写入到第一个块的末尾
    println!("Writing data at offset: {}", offset);
    let bytes_written = fs.write_file_blocks(ino, offset, data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);

    // 读取数据
    println!("Reading data at offset: {}", offset);
    let read_data = fs.read_file(ino, offset, data.len() as u32, uid, gid).await.expect("Failed to read file");
    assert_eq!(read_data, data);

    // 写入数据跨越块边界
    let data2 = b"Crossing boundary data";
    let offset2 = 4096 - (data2.len() as i64 / 2); // 跨越第一个和第二个块
    println!("Writing data2 at offset2: {}", offset2);
    let bytes_written2 = fs.write_file_blocks(ino, offset2, data2, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written2, data2.len() as u64);

    // 读取跨越块边界的数据
    println!("Reading data2 at offset2: {}", offset2);
    let read_data2 = fs.read_file(ino, offset2, data2.len() as u32, uid, gid).await.expect("Failed to read file");
    assert_eq!(read_data2, data2);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_verify_file_attr() {
    println!("Starting test_write_and_verify_file_attr");
    let fs = create_test_redisfs().await;
    let parent = 1; // root directory
    let name = OsString::from("test_write_attr.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // root user
    let gid = 0;  // root group

    // Create file
    let (ino, attr_before) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 等待一小段时间，确保时间戳有变化
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Write data
    let data = b"Hello, World!";
    let bytes_written = fs.write_file_blocks(ino, 0, data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);

    // Get file attributes after write
    let attr_after = fs.get_attr(ino).await.expect("Failed to get file attributes");

    // Verify file size
    assert_eq!(attr_after.size, data.len() as u64);
    assert!(attr_after.size > attr_before.size);

    // Verify modification time
    assert!(attr_after.mtime > attr_before.mtime);

    // Verify change time
    assert!(attr_after.ctime > attr_before.ctime);

    // Delete file
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_read_non_aligned_offset() {
    println!("Starting test_write_and_read_non_aligned_offset");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_non_aligned_offset.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入初始数据
    let initial_data = b"Initial data";
    fs.write_file_blocks(ino, 0, initial_data, uid, gid).await.expect("Failed to write initial data");
    println!("Initial data written: {:?}", initial_data);

    // 立即验证初始数据
    let read_initial = fs.read_file(ino, 0, initial_data.len() as u32, uid, gid).await.expect("Failed to read initial data");
    println!("Initial data read: {:?}", read_initial);
    assert_eq!(read_initial, initial_data, "Initial data verification failed");

    // 获取文件属性
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    println!("File size after initial write: {}", attr.size);

    // 在非对齐偏移量2000处写入数据
    let offset1 = 2000;
    let data1 = b"First non-aligned write test";
    fs.write_file_blocks(ino, offset1, data1, uid, gid).await.expect("Failed to write file at first non-aligned offset");
    println!("Data1 written at offset {}: {:?}", offset1, data1);

    // 立即验证第一次写入
    let read_data_initial = fs.read_file(ino, 0, initial_data.len() as u32, uid, gid).await.expect("Failed to read first non-aligned data");
    println!("Data1 read: {:?}", read_data_initial);
    assert_eq!(read_data_initial, initial_data, "First non-aligned write verification failed");

    // 验证第一次写入
    let read_data1 = fs.read_file(ino, offset1, data1.len() as u32, uid, gid).await.expect("Failed to read first non-aligned data");
    println!("Data1 read: {:?}", read_data1);
    assert_eq!(read_data1, data1, "First non-aligned write verification failed");

    // 在非对齐偏移量3000处写入数据
    let offset2 = 3000;
    let data2 = b"Second non-aligned write test";
    fs.write_file_blocks(ino, offset2, data2, uid, gid).await.expect("Failed to write file at second non-aligned offset");
    println!("Data2 written at offset {}: {:?}", offset2, data2);

    // 验证第二次写入
    let read_data2 = fs.read_file(ino, offset2, data2.len() as u32, uid, gid).await.expect("Failed to read second non-aligned data");
    println!("Data2 read: {:?}", read_data2);
    assert_eq!(read_data2, data2, "Second non-aligned write verification failed");

    // Overwrite part of the hole
    let data3 = b"Overwriting hole";
    let offset3 = 4096i64; // In the middle of the hole
    fs.write_file_blocks(ino, offset3, data3, uid, gid).await.expect("Failed to write third chunk");
    println!("Data3 written at offset {}: {:?}", offset3, data3);

    // Get updated file attributes
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    let file_size = attr.size;
    println!("File size after all writes: {}", file_size);

    // Read entire file again
    let read_data = fs.read_file(ino, 0, file_size as u32, uid, gid).await.expect("Failed to read entire file");
    //println!("Full file content: {:?}", read_data);
    println!("Read data length: {}", read_data.len());

    // Verify all data
    assert_eq!(&read_data[..initial_data.len()], initial_data, "Initial data in full read doesn't match");
    assert_eq!(&read_data[offset1 as usize..offset1 as usize + data1.len()], data1, "First non-aligned data in full read doesn't match");
    assert_eq!(&read_data[offset3 as usize..offset3 as usize + data3.len()], data3, "Third data (overwriting hole) in full read doesn't match");
    assert_eq!(&read_data[offset2 as usize..offset2 as usize + data2.len()], data2, "Second non-aligned data in full read doesn't match");

    // Verify remaining holes
    assert!(read_data[initial_data.len()..offset1 as usize].iter().all(|&x| x == 0), "First hole is not all zeros");
    assert!(read_data[offset1 as usize + data1.len()..offset2 as usize].iter().all(|&x| x == 0), "Second hole (before overwrite) is not all zeros");
    assert!(read_data[offset2 as usize + data2.len()..offset3 as usize].iter().all(|&x| x == 0), "Third hole (after overwrite) is not all zeros");
    assert!(read_data[offset3 as usize + data3.len()..].iter().all(|&x| x == 0), "Hole after overwrite is not all zeros");
    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_read_with_offset() {
    println!("Starting test_write_and_read_with_offset");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_offset.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入初始数据
    let initial_data = b"Hello, World!";
    let bytes_written = fs.write_file_blocks(ino, 0, initial_data, uid, gid).await.expect("Failed to write initial data");
    assert_eq!(bytes_written, initial_data.len() as u64);
    
    let data = fs.read_file(ino, 0, initial_data.len() as u32, uid, gid).await.expect("Failed to read initial data");
    assert_eq!(data, initial_data);
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, initial_data.len() as u64);
    
    // 在偏移量处写入新数据
    let offset = 7;
    let new_data = b"Redis";
    let bytes_written = fs.write_file_blocks(ino, offset, new_data, uid, gid).await.expect("Failed to write new data");
    assert_eq!(bytes_written, new_data.len() as u64);
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, initial_data.len() as u64);

    // 读取整个文件
    let read_data = fs.read_file(ino, 0, (initial_data.len() + new_data.len()) as u32, uid, gid).await.expect("Failed to read file");
    
    // 验证数据
    let expected_data = b"Hello, Redis!";
    assert_eq!(read_data, expected_data);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_beyond_file_size() {
    println!("Starting test_write_beyond_file_size");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_beyond.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入初始数据
    let initial_data = b"Short";
    let bytes_written = fs.write_file_blocks(ino, 0, initial_data, uid, gid).await.expect("Failed to write initial data");
    assert_eq!(bytes_written, initial_data.len() as u64);

    // 在文件末尾之后写入新数据
    let offset = 10;
    let new_data = b"Long data";
    let bytes_written = fs.write_file_blocks(ino, offset, new_data, uid, gid).await.expect("Failed to write new data");
    assert_eq!(bytes_written, new_data.len() as u64);

    // 读取整个文件
    let read_data = fs.read_file(ino, 0, (offset + new_data.len() as i64) as u32, uid, gid).await.expect("Failed to read file");
    
    // 验证数据
    let mut expected_data = vec![0; offset as usize];
    expected_data[..initial_data.len()].copy_from_slice(initial_data);
    expected_data.extend_from_slice(new_data);
    assert_eq!(read_data, expected_data);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_multiple_writes_and_reads() {
    println!("Starting test_multiple_writes_and_reads");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_multiple.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 多次写入数据
    let data1 = b"First ";
    let data2 = b"Second ";
    let data3 = b"Third";

    fs.write_file_blocks(ino, 0, data1, uid, gid).await.expect("Failed to write data1");
    fs.write_file_blocks(ino, data1.len() as i64, data2, uid, gid).await.expect("Failed to write data2");
    fs.write_file_blocks(ino, (data1.len() + data2.len()) as i64, data3, uid, gid).await.expect("Failed to write data3");

    // 读取整个文件
    let read_data = fs.read_file(ino, 0, (data1.len() + data2.len() + data3.len()) as u32, uid, gid).await.expect("Failed to read file");
    
    // 验证数据
    let expected_data = b"First Second Third";
    assert_eq!(read_data, expected_data);

    // 部分读取
    let partial_read = fs.read_file(ino, 6, 6, uid, gid).await.expect("Failed to perform partial read");
    assert_eq!(partial_read, b"Second");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_read_large_file_with_holes() {
    println!("Starting test_write_and_read_large_file_with_holes");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_large_holes.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用户
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入大文件，包含空洞
    let data1 = vec![1u8; 4096]; // 4KB
    let data2 = vec![2u8; 4096]; // 4KB
    let hole_size = 1024 * 1024; // 1MB

    fs.write_file_blocks(ino, 0, &data1, uid, gid).await.expect("Failed to write data1");
    fs.write_file_blocks(ino, (data1.len() + hole_size) as i64, &data2, uid, gid).await.expect("Failed to write data2");

    // 读取整个文件
    let read_data = fs.read_file(ino, 0, (data1.len() + hole_size + data2.len()) as u32, uid, gid).await.expect("Failed to read file");
    
    // 验证数据
    assert_eq!(read_data[..data1.len()], data1);
    assert_eq!(read_data[data1.len()..data1.len() + hole_size], vec![0u8; hole_size]);
    assert_eq!(read_data[data1.len() + hole_size..], data2);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_read_and_verify_file_size() {
    println!("Starting test_write_read_and_verify_file_size");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_file_size.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // 使用root用
    let gid = 0;  // 使用root组

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入数据
    let data = b"Hello, World! This is a test file.";
    let bytes_written = fs.write_file_blocks(ino, 0, data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);

    // 获取文件属性
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    println!("File size after write: {} bytes", attr.size);

    // 验证文件大小
    assert_eq!(attr.size, data.len() as u64, "File size does not match written data length");

    // 读取文件内容
    let read_data = fs.read_file(ino, 0, attr.size as u32, uid, gid).await.expect("Failed to read file");

    // 验证文件内容
    assert_eq!(read_data, data, "Read data does not match written data");

    // 再次获取文件属性，确保大小没有变化
    let attr_after_read = fs.get_attr(ino).await.expect("Failed to get file attributes after read");
    assert_eq!(attr_after_read.size, attr.size, "File size changed after read");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_and_read_with_overlapping_blocks() {
    println!("开始测试：跨块写入和读取");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_overlapping_blocks.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入跨越多个块的数据
    let data = vec![0xAA; 8192]; // 8KB 数据，跨越两个 4KB 块
    let bytes_written = fs.write_file_blocks(ino, 0, &data, uid, gid).await.expect("Failed to write file");
    assert_eq!(bytes_written, data.len() as u64);
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, data.len() as u64);

    // 读取并验证数据
    let read_data = fs.read_file(ino, 0, data.len() as u32, uid, gid).await.expect("Failed to read file");
    assert_eq!(read_data, data, "Read data does not match written data");
    assert_eq!(attr.size, data.len() as u64);
    // assert_eq!(attr.blocks, 2);
    assert_eq!(read_data, data);

    // 写入部分覆盖两个块的数据
    let overlap_data = vec![0xBB; 2048]; // 2KB 数据
    let overlap_offset = 3072; // 从第一个块的 3KB 处开始写入
    fs.write_file_blocks(ino, overlap_offset, &overlap_data, uid, gid).await.expect("Failed to write overlapping data");

    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, data.len() as u64);

    // 读取并验证覆盖后的数据
    let read_data = fs.read_file(ino, 0, data.len() as u32, uid, gid).await.expect("Failed to read file after overlap write");
    assert_eq!(&read_data[..overlap_offset as usize], &data[..overlap_offset as usize], "Data before overlap should remain unchanged");
    assert_eq!(&read_data[overlap_offset as usize..(overlap_offset + overlap_data.len() as i64) as usize], &overlap_data, "Overlapped data should match written data");
    assert_eq!(&read_data[(overlap_offset + overlap_data.len() as i64) as usize..], &data[(overlap_offset + overlap_data.len() as i64) as usize..], "Data after overlap should remain unchanged");

    // 验证文件大小和块数
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, data.len() as u64, "File size incorrect");
    let expected_blocks = (data.len() as u64 + 4095) / 4096;
    assert_eq!(attr.blocks, expected_blocks, "Block count incorrect");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_read_with_non_block_aligned_sizes() {
    println!("开始测试：非块对齐大小的写入和读取");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_non_aligned_sizes.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 写入非块对齐大小的数据
    let data1 = vec![0xCC; 3000]; // 3000 字节，不是 4KB 的倍数
    let bytes_written = fs.write_file_blocks(ino, 0, &data1, uid, gid).await.expect("Failed to write first data");
    assert_eq!(bytes_written, data1.len() as u64);

    // 读取并验证数据
    let read_data1 = fs.read_file(ino, 0, data1.len() as u32, uid, gid).await.expect("Failed to read first data");
    assert_eq!(read_data1, data1, "Read data1 does not match written data1");

    // 写入另一个非块对齐大小的数据，部分覆盖第一次写入的数据
    let data2 = vec![0xDD; 2500]; // 2500 字节
    let offset2 = 2000; // 从 2000 字节处开始写入
    fs.write_file_blocks(ino, offset2, &data2, uid, gid).await.expect("Failed to write second data");

    // 读取并验证最终数据
    let total_size = (offset2 + data2.len() as i64) as u32;
    let read_data_final = fs.read_file(ino, 0, total_size, uid, gid).await.expect("Failed to read final data");

    assert_eq!(&read_data_final[..offset2 as usize], &data1[..offset2 as usize], "Data before second write should remain unchanged");
    assert_eq!(&read_data_final[offset2 as usize..], &data2, "Data after second write should match data2");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_read_with_large_offset() {
    println!("开始测试：大偏移量的写入和读取");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_large_offset.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 在大偏移量处写入数据
    let large_offset = 1_000_000; // 1MB 偏移
    let data = vec![0xEE; 4096]; // 4KB 数据
    fs.write_file_blocks(ino, large_offset, &data, uid, gid).await.expect("Failed to write data at large offset");

    // 读取并验证数据
    let read_data = fs.read_file(ino, large_offset, data.len() as u32, uid, gid).await.expect("Failed to read data from large offset");
    assert_eq!(read_data, data, "Read data does not match written data at large offset");

    // 验证文件大小
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, (large_offset + data.len() as i64) as u64, "File size incorrect after write at large offset");

    // 读取文件开始到写入数据之间的内容（应该是空洞）
    let hole_data = fs.read_file(ino, 0, large_offset as u32, uid, gid).await.expect("Failed to read hole data");
    assert!(hole_data.iter().all(|&x| x == 0), "Hole data should be all zeros");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_multiple_small_writes_and_reads() {
    println!("开始测试：多次小规模写入和读取");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_multiple_small_writes.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    let mut expected_data = Vec::new();
    let write_count = 100;
    let write_size = 100;

    // 执行多次小规模写入
    for i in 0..write_count {
        let data = vec![(i % 256) as u8; write_size];
        let offset = i * write_size;
        fs.write_file_blocks(ino, offset as i64, &data, uid, gid).await.expect("Failed to write small data");
        expected_data.extend_from_slice(&data);
    }

    // 读取整个文件并验证
    let total_size = write_count * write_size;
    let read_data = fs.read_file(ino, 0, total_size as u32, uid, gid).await.expect("Failed to read entire file");
    assert_eq!(read_data, expected_data, "Read data does not match expected data after multiple small writes");

    // 随机读取验证
    for _ in 0..10 {
        let random_offset = rand::thread_rng().gen_range(0..(total_size - write_size));
        let read_data = fs.read_file(ino, random_offset as i64, write_size as u32, uid, gid).await.expect("Failed to read random segment");
        assert_eq!(read_data, &expected_data[random_offset..random_offset + write_size], "Random read data does not match expected data");
    }

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_read_with_sparse_file() {
    println!("开始测试：稀疏文件的写入和读取");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_sparse_file.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 在不同的偏移量写入数据，创建稀疏文件
    let data1 = vec![0xAA; 1000];
    let offset1 = 0;
    let data2 = vec![0xBB; 1000];
    let offset2 = 10000;
    let data3 = vec![0xCC; 1000];
    let offset3 = 20000;

    fs.write_file_blocks(ino, offset1, &data1, uid, gid).await.expect("Failed to write data1");
    fs.write_file_blocks(ino, offset2, &data2, uid, gid).await.expect("Failed to write data2");
    fs.write_file_blocks(ino, offset3, &data3, uid, gid).await.expect("Failed to write data3");

    // 验证文件大小
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, (offset3 + data3.len() as i64) as u64, "File size incorrect");

    // 读取并验证数据
    let read_data1 = fs.read_file(ino, offset1, data1.len() as u32, uid, gid).await.expect("Failed to read data1");
    let read_data2 = fs.read_file(ino, offset2, data2.len() as u32, uid, gid).await.expect("Failed to read data2");
    let read_data3 = fs.read_file(ino, offset3, data3.len() as u32, uid, gid).await.expect("Failed to read data3");

    assert_eq!(read_data1, data1, "Data1 mismatch");
    assert_eq!(read_data2, data2, "Data2 mismatch");
    assert_eq!(read_data3, data3, "Data3 mismatch");

    // 读取空洞部分
    let hole_data = fs.read_file(ino, data1.len() as i64, (offset2 - data1.len() as i64) as u32, uid, gid).await.expect("Failed to read hole data");
    assert!(hole_data.iter().all(|&x| x == 0), "Hole should contain all zeros");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_complex_write_read_patterns() {
    println!("开始测试：复杂的写入和读取模式");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_complex_patterns.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 复杂的写入模式
    let patterns = vec![
        (0, vec![0xAA; 1000]),
        (2000, vec![0xBB; 500]),
        (1500, vec![0xCC; 1000]),
        (3000, vec![0xDD; 2000]),
        (500, vec![0xEE; 700]),
    ];

    for (offset, data) in &patterns {
        fs.write_file_blocks(ino, *offset, data, uid, gid).await.expect("Failed to write data");
    }

    // 验证文件大小
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    let expected_size = patterns.iter().map(|(offset, data)| offset + data.len() as i64).max().unwrap() as u64;
    assert_eq!(attr.size, expected_size, "File size incorrect after complex writes");

    // 读取并验证数据
    let read_data = fs.read_file(ino, 0, expected_size as u32, uid, gid).await.expect("Failed to read entire file");

    // 验证每个写入的数据段
    //for (offset, data) in &patterns {
    //    assert_eq!(&read_data[*offset as usize..*offset as usize + data.len()], data, "Data mismatch at offset {}", offset);
    //}

    // 验证未写入的区域（应该为0）
    let mut expected_data = vec![0; expected_size as usize];
    for (offset, data) in &patterns {
        expected_data.splice(*offset as usize..*offset as usize + data.len(), data.iter().cloned());
    }
    assert_eq!(read_data, expected_data, "Overall data mismatch");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_read_with_varying_block_sizes() {
    println!("开始测试：不同块大小的写入和读取");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_varying_blocks.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    let block_size = 4096; // 假设块大小为4KB
    let sizes = vec![
        block_size / 2,
        block_size - 1,
        block_size,
        block_size + 1,
        block_size * 2,
        block_size * 2 + 500,
    ];

    let mut offset = 0;
    let mut expected_data = Vec::new();

    for (i, size) in sizes.iter().enumerate() {
        let data = vec![(i % 256) as u8; *size];
        fs.write_file_blocks(ino, offset, &data, uid, gid).await.expect("Failed to write data");
        expected_data.extend_from_slice(&data);
        offset += *size as i64;
    }

    // 验证文件大小
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, offset as u64, "File size incorrect after writes");

    // 读取并验证整个文件
    let read_data = fs.read_file(ino, 0, offset as u32, uid, gid).await.expect("Failed to read entire file");
    assert_eq!(read_data, expected_data, "Read data does not match expected data");

    // 随机读取验证
    let mut rng = rand::thread_rng();
    for _ in 0..10 {
        let random_offset = rng.gen_range(0..offset);
        let random_size = rng.gen_range(1..1000).min((offset - random_offset) as usize);
        let read_data = fs.read_file(ino, random_offset, random_size as u32, uid, gid).await.expect("Failed to read random segment");
        assert_eq!(read_data, &expected_data[random_offset as usize..random_offset as usize + random_size], "Random read data does not match expected data");
    }

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_write_read_with_file_expansion_and_truncation() {
    println!("开始测试：文件扩展和截断");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_expand_truncate.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 初始写入
    let initial_data = vec![0xAA; 5000];
    fs.write_file_blocks(ino, 0, &initial_data, uid, gid).await.expect("Failed to write initial data");

    // 验证初始大小
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, 5000, "Initial file size incorrect");

    // 扩展文件
    let expand_data = vec![0xBB; 3000];
    fs.write_file_blocks(ino, 8000, &expand_data, uid, gid).await.expect("Failed to expand file");

    // 验证扩展后的大小
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, 11000, "File size incorrect after expansion");

    // 读取并验证数据
    let read_data = fs.read_file(ino, 0, 11000, uid, gid).await.expect("Failed to read expanded file");
    assert_eq!(&read_data[..5000], &initial_data, "Initial data corrupted");
    assert_eq!(&read_data[5000..8000], &vec![0; 3000], "Gap not filled with zeros");
    assert_eq!(&read_data[8000..], &expand_data, "Expanded data incorrect");

    // 截断文件（模拟）
    // 注意：这里假设有一个truncate方法，实际实现可能需要单独添加
    // fs.truncate(ino, 6000, uid, gid).await.expect("Failed to truncate file");

    // 验证截断后的大小
    // let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    // assert_eq!(attr.size, 6000, "File size incorrect after truncation");

    // 读取并验证截断后的数据
    // let read_data = fs.read_file(ino, 0, 6000, uid, gid).await.expect("Failed to read truncated file");
    // assert_eq!(&read_data[..5000], &initial_data, "Data before truncation point corrupted");
    // assert_eq!(&read_data[5000..], &vec![0; 1000], "Data after initial data should be zeros");

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}

#[tokio::test]
async fn test_file_blocks_attribute() {
    println!("开始测试：文件块数属性");
    let fs = create_test_redisfs().await;
    let parent = 1; // 根目录
    let name = OsString::from("test_blocks.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;
    let gid = 0;

    // 创建文件
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // 测试不同大小的写入
    let test_sizes = vec![100, 512, 1000, 4096, 8192, 10000];

    for &size in &test_sizes {
        let data = vec![0xAA; size];
        fs.write_file_blocks(ino, 0, &data, uid, gid).await.expect("Failed to write data");

        let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
        assert_eq!(attr.size, size as u64, "File size incorrect for size {}", size);

        let expected_blocks = (size as u64 + 4095) / 4096; // 向上取整到4096字节块
        assert_eq!(attr.blocks, expected_blocks, "Block count incorrect for size {}", size);

        println!("Size: {}, Blocks: {}, Expected Blocks: {}", size, attr.blocks, expected_blocks);
    }

    // 测试稀疏文件
    let sparse_offset = 1_000_000; // 1MB
    let sparse_data = vec![0xBB; 1000];
    fs.write_file_blocks(ino, sparse_offset, &sparse_data, uid, gid).await.expect("Failed to write sparse data");

    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    assert_eq!(attr.size, (sparse_offset + sparse_data.len() as i64) as u64, "Sparse file size incorrect");

    // 计算总的预期块数：之前写入的最大数据 + 稀疏数据
    let max_previous_size = *test_sizes.iter().max().unwrap() as u64;
    let expected_blocks = (max_previous_size + 4095) / 4096 + (sparse_data.len() as u64 + 4095) / 4096;
    assert_eq!(attr.blocks, expected_blocks, "Sparse file block count incorrect");

    println!("Sparse file size: {}, Blocks: {}, Expected Blocks: {}", attr.size, attr.blocks, expected_blocks);

    // 删除文件
    fs.remove_file(parent, &name, uid, gid).await.expect("Failed to remove file");
}