mod parsing;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = server::BurrowMQServer::new();
    server.start_forever().await?;
    Ok(())
}
