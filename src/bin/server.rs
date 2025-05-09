use bytes::Bytes;
use burrow_mq::server;
use clap::Parser;
use env_logger::Builder;
use log::LevelFilter;
use burrow_mq::queue::lock_free::LockFreeQueue;
use burrow_mq::server::BurrowMQServer;

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

    let server: BurrowMQServer<LockFreeQueue<Bytes>> = BurrowMQServer::new();
    server.start_forever(args.port).await?;
    Ok(())
}
