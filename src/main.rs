mod server;
mod parsing;


// #[derive(Clone)]
// struct MyData<'a> {
//     x: String,
//     xx: &'a str
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut server = server::BurrowMQServer::new();
    server.start_forever().await?;
    Ok(())
}
