use super::*;
use tokio;
use std::ffi::OsString;
use tokio::fs::remove_dir_all;
use redis::AsyncCommands; // Ensure the necessary traits are in scope

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
async fn test_sparse_write_and_read() {
    println!("Starting test_sparse_write_and_read");
    let fs = create_test_redisfs().await;
    let parent = 1; // root directory
    let name = OsString::from("test_sparse.txt");
    let mode = 0o644;
    let umask = 0o022;
    let flags = 0;
    let uid = 0;  // root user
    let gid = 0;  // root group

    // Create file
    let (ino, _) = fs.create_file(parent, &name, mode, umask, flags, uid, gid).await.expect("Failed to create file");

    // Write data with holes
    let data1 = b"First chunk of data";
    let data2 = b"Second chunk of data";
    let offset1 = 0i64;
    let offset2 = 8192i64; // 2 blocks away

    fs.write_file_blocks(ino, offset1, data1, uid, gid).await.expect("Failed to write first chunk");
    fs.write_file_blocks(ino, offset2, data2, uid, gid).await.expect("Failed to write second chunk");

    // Get file attributes to check the actual file size
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    let file_size = attr.size;

    println!("File size after writes: {}", file_size);

    // Read entire file
    let read_data = fs.read_file(ino, 0, file_size as u32, uid, gid).await.expect("Failed to read file");

    println!("Read data length: {}", read_data.len());

    // Verify data
    assert_eq!(&read_data[offset1 as usize..offset1 as usize + data1.len()], data1);
    assert_eq!(&read_data[offset2 as usize..offset2 as usize + data2.len()], data2);

    // Verify holes
    assert!(read_data[data1.len()..offset2 as usize].iter().all(|&x| x == 0));

    // Overwrite part of the hole
    let data3 = b"Overwriting hole";
    let offset3 = 4096i64; // In the middle of the hole
    fs.write_file_blocks(ino, offset3, data3, uid, gid).await.expect("Failed to write third chunk");

    // Get updated file attributes
    let attr = fs.get_attr(ino).await.expect("Failed to get file attributes");
    let file_size = attr.size;

    println!("File size after overwrite: {}", file_size);

    // Read entire file again
    let read_data = fs.read_file(ino, 0, file_size as u32, uid, gid).await.expect("Failed to read file");

    println!("Read data length after overwrite: {}", read_data.len());

    // Verify all data
    assert_eq!(&read_data[offset1 as usize..offset1 as usize + data1.len()], data1);
    assert_eq!(&read_data[offset3 as usize..offset3 as usize + data3.len()], data3);
    assert_eq!(&read_data[offset2 as usize..offset2 as usize + data2.len()], data2);

    // Verify remaining holes
    assert!(read_data[data1.len()..offset3 as usize].iter().all(|&x| x == 0));
    assert!(read_data[offset3 as usize + data3.len()..offset2 as usize].iter().all(|&x| x == 0));

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