mod parsing;
mod server;

// #[derive(Clone)]
// struct MyData<'a> {
//     x: String,
//     xx: &'a str
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = server::BurrowMQServer::new();
    server.start_forever().await?;
    Ok(())
}
