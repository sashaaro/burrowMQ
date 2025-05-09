use burrow_mq::queue::lock_free::LockFreeQueue;
use burrow_mq::server::BurrowMQServer;
use bytes::Bytes;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;

#[derive(clap::Parser)]
struct CliArgs {
    #[clap(short, long, default_value = "5672")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Builder::new()
        .filter(Some("burrow_mq"), LevelFilter::Info)
        .init();

    let args = CliArgs::parse();

    let server: BurrowMQServer<crossbeam_queue::SegQueue<Bytes>> = BurrowMQServer::new();
    server.start_forever(args.port).await?;
    Ok(())
}
