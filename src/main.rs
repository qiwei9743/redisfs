mod fs;

use clap::{Parser, Subcommand, Args};
use fuser::MountOption;
use std::path::PathBuf;
use std::error::Error;
use crate::fs::redisfs::{RedisFs, RedisFsck};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
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
    Fsck(FsckArgs),
}

#[derive(Args)]
struct FsckArgs {
    #[arg(short, long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
    #[arg(short, long, help = "Use root-based consistency check")]
    from_root: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Mount { redis_url, mountpoint } => {
            // 创建RedisFs实例
            let redis_fs = match RedisFs::new(redis_url).await {
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
            fuser::mount2(redis_fs, mountpoint, &options)?;
            println!("RedisFs successfully mounted at {}", mountpoint.display());
        },
        Commands::Unmount { mountpoint } => {
            // 实现卸载逻辑
            println!("Unmounting {}", mountpoint.display());
            // TODO: 实现卸载逻辑
        },
        Commands::Fsck(args) => {
            // 创建 RedisFsck 实例
            let fsck = RedisFsck::new(&args.redis_url).await?;

            if args.from_root {
                // 执行从根开始的一致性检查
                match fsck.check_consistency_from_root().await {
                    Ok(_) => println!("File system check and repair completed"),
                    Err(e) => eprintln!("File system check and repair failed: {}", e),
                }
            } else {
                // 执行常规的文件系统检查和修复
                match fsck.check_and_repair().await {
                    Ok(_) => println!("File system check and repair completed"),
                    Err(e) => eprintln!("File system check and repair failed: {}", e),
                }
            }
        },
    }

    Ok(())
}
