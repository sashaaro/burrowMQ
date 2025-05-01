use burrow_mq::server;
use lapin::options::BasicQosOptions;
use lapin::{Connection, ConnectionProperties};
use std::time::Duration;
use tokio::time::sleep;

mod dsl;

#[tokio::test]
async fn main_test() -> anyhow::Result<()> {
    //console_subscriber::init();

    let param = std::env::var("NO_EMBEDDED_AMQP").unwrap_or_default();
    if param == "" || param == "0" || param == "false" {
        tokio::spawn(async {
            let server = server::BurrowMQServer::new();
            server.start_forever(5672).await.expect("Server failed");
        });
        sleep(Duration::from_millis(200)).await;
    }

    let addr = "amqp://127.0.0.1:5672/%2f";
    let connection = Connection::connect(addr, ConnectionProperties::default())
        .await
        .expect("Connection failed");
    println!("Connected to RabbitMQ");

    let channel = connection.create_channel().await.expect("Channel failed"); // channel #1
    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .expect("failed to set qos");

    // publish message to queue via routing key, consume message
    dsl::load_scenario(
        r"
queue.declare name='messages_queue'
queue.purge name='messages_queue'
basic.publish routing_key='messages_queue' body='HELLO FROM LAPIN!'
expect.consume queue='messages_queue' body='HELLO FROM LAPIN!'
basic.ack
    ",
    )
    .run(&channel)
    .await;

    dsl::load_scenario(
        r"
queue.declare name='messages_queue'
queue.purge name='messages_queue'
basic.publish routing_key='messages_queue' body='NEW MESSAGE FROM LAPIN!'
basic.publish routing_key='messages_queue' body='MESSAGE #2 FROM LAPIN!'
expect.consume queue='messages_queue' body='NEW MESSAGE FROM LAPIN!'
basic.ack
expect.consume queue='messages_queue' body='MESSAGE #2 FROM LAPIN!'
",
    )
    .run(&channel)
    .await;

    // publish a message to exchange, consume a message from bound queue
    dsl::load_scenario(
        r"
exchange.declare name='logs'
queue.declare name='logs_queue'
queue.bind queue='logs_queue' exchange='logs'
basic.publish exchange='logs' body='log message'
expect.consume queue='logs_queue' body='log message'
basic.ack
",
    )
    .run(&channel)
    .await;

    Ok(())
}
