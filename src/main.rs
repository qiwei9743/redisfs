
mod fs;

fn main() {
    env_logger::init();

    use fuser::MountOption;
    use std::env;
    use std::path::Path;

    // 从环境变量获取Redis URL，如果没有设置则使用默认值
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    // 创建RedisFs实例
    let redis_fs = match fs::redisfs::RedisFs::new(&redis_url) {
        Ok(fs) => fs,
        Err(e) => {
            eprintln!("无法创建RedisFs实例: {}", e);
            std::process::exit(1);
        }
    };

    // 获取挂载点参数
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("请指定挂载点");
        return;
    }
    let mountpoint = Path::new(&args[1]);

    // 挂载选项
    let options = vec![
        MountOption::RW, 
        MountOption::FSName("redisfs".to_string()), 
        MountOption::AutoUnmount, 
        MountOption::AllowOther];

    // 挂载文件系统
    match fuser::mount2(redis_fs, &mountpoint, &options) {
        Ok(_) => println!("RedisFs已成功挂载到 {}", mountpoint.display()),
        Err(e) => eprintln!("挂载RedisFs失败: {}", e),
    }
}
