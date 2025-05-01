mod parsing;
mod server;

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
    Builder::new().filter(None, LevelFilter::Info).init();

    let args = CliArgs::parse();

    let server = server::BurrowMQServer::new();
    server.start_forever(args.port).await?;
    Ok(())
}
