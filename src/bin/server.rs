use std::time::Duration;
use burrow_mq::server::BurrowMQServer;
use bytes::Bytes;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use tokio::sync::Notify;
use tokio::time::sleep;

#[derive(clap::Parser)]
struct CliArgs {
    #[clap(short, long, default_value = "5672")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let a = Notify::new();
    a.notify_one();
    a.notify_one();
    println!("1");
    sleep(Duration::from_secs(1)).await;

    a.notified().await;
    println!("22");
    // 
    // sleep(Duration::from_secs(1)).await;
    // a.notified().await;
    // println!("333");
    
    Builder::new()
        .filter(Some("burrow_mq"), LevelFilter::Info)
        .init();

    let args = CliArgs::parse();

    let server: BurrowMQServer<crossbeam_queue::SegQueue<Bytes>> = BurrowMQServer::new();
    server.start_forever(args.port).await?;
    Ok(())
}
