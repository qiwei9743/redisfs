mod fs;

use clap::Parser;
use fuser::MountOption;
use std::path::PathBuf;
use std::error::Error;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    Mount {
        #[arg(short, long, default_value = "redis://127.0.0.1:6379")]
        redis_url: String,
        #[arg(short, long)]
        mountpoint: PathBuf,
    },
    Unmount {
        #[arg(short, long)]
        mountpoint: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Mount { redis_url, mountpoint } => {
            // 创建RedisFs实例
            let redis_fs = match fs::redisfs::RedisFs::new(redis_url).await {
                Ok(fs) => fs,
                Err(e) => {
                    eprintln!("无法创建RedisFs实例: {}", e);
                    std::process::exit(1);
                }
            };

            // 挂载选项
            let options = vec![
                MountOption::RW,
                MountOption::FSName("redisfs".to_string()),
                MountOption::AutoUnmount,
                MountOption::AllowOther
            ];

            // 挂载文件系统
            fuser::mount2(redis_fs, &mountpoint, &options)?;
            println!("RedisFs已成功挂载到 {}", mountpoint.display());
        },
        Commands::Unmount { mountpoint } => {
            // 执行卸载操作
            match std::process::Command::new("fusermount")
                .arg("-u")
                .arg(mountpoint)
                .status()
            {
                Ok(status) if status.success() => println!("RedisFs已成功从 {} 卸载", mountpoint.display()),
                Ok(_) => eprintln!("卸载RedisFs失败"),
                Err(e) => eprintln!("执行卸载命令失败: {}", e),
            }
        },
    }

    Ok(())
}
