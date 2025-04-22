mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut server = server::BurrowMQServer::new();
    server.start_forever().await?;
    Ok(())
}
