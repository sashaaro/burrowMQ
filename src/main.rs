mod parsing;
mod server;

use clap::Parser;
use env_logger::Builder;
use futures_util::TryFutureExt;
use log::LevelFilter;
use std::panic::resume_unwind;
use std::process;
use std::thread::sleep;
use std::time::Duration;
use tokio::time::sleep as tokio_sleep;

#[derive(clap::Parser)]
struct CliArgs {
    #[clap(short, long, default_value = "5672")]
    port: u16,
}

fn main() {
    // std::panic::set_hook(Box::new(|info| {
    //     eprintln!("Panic occurred: {:?}", info);
    //     process::exit(1); // Завершаем процесс
    // }));

    std::thread::spawn(|| {
        panic!("1123");
    });
    for i in 0..10 {
        sleep(Duration::from_millis(500));
    }
    println!("khe hke");
}

#[tokio::main]
async fn main1() -> anyhow::Result<()> {
    // let a = tokio::spawn(async {
    //     panic!("1");
    // });
    //
    // a.map_err(|e| {
    //     if e.is_panic() {
    //         std::panic::resume_unwind(Box::new(e))
    //     }
    // });
    //
    // for i in 0..10 {
    //     tokio_sleep(Duration::from_millis(500)).await;
    // }
    // println!("khe hke");
    //
    // return Ok(());

    Builder::new().filter(None, LevelFilter::Info).init();

    let args = CliArgs::parse();

    let server = server::BurrowMQServer::new();
    server.start_forever(args.port).await?;
    Ok(())
}
