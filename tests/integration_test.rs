use burrowMQ::server;
use futures_lite::stream::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use tokio;

#[tokio::test]
async fn test_connect() -> anyhow::Result<()> {
    tokio::spawn(async {
        let mut server = server::BurrowMQServer::new();
        server.start_forever().await.expect("Server failed");
    });

    let addr = "amqp://127.0.0.1:5672/%2f";
    let connection = Connection::connect(addr, ConnectionProperties::default())
        .await?;
    println!("Connected to RabbitMQ");

    // Открываем канал
    let channel = connection.create_channel().await.expect("Channel failed");

    // Объявляем очередь
    let queue = channel
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Queue declare failed");

    println!("Declared queue {:?}", queue.name());

    // Публикуем сообщение
    let payload = b"Hello from Rust with lapin!";
    channel
        .basic_publish(
            "",
            "hello",
            BasicPublishOptions::default(),
            &*payload.to_vec(),
            BasicProperties::default(),
        )
        .await
        .expect("Publish failed")
        .await
        .expect("Publish confirmation failed");

    println!("Sent: {:?}", std::str::from_utf8(payload).unwrap());

    // Подписываемся на очередь
    let mut consumer = channel
        .basic_consume(
            "hello",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Consume failed");

    println!("Waiting for messages...");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            let data = std::str::from_utf8(&delivery.data).unwrap();
            println!("Received: {}", data);
            delivery.ack(Default::default()).await.expect("Ack failed");
        }
    }
    Ok(())
}
