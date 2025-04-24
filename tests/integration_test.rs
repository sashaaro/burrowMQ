use burrowMQ::server;
use futures_lite::stream::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn main_test() -> anyhow::Result<()> {
    tokio::spawn(async {
        let server = server::BurrowMQServer::new();
        server.start_forever(5672).await.expect("Server failed");
    });
    sleep(Duration::from_millis(200)).await;

    let addr = "amqp://127.0.0.1:5672/%2f";
    let connection = Connection::connect(addr, ConnectionProperties::default())
        .await
        .expect("Connection failed");
    println!("Connected to RabbitMQ");

    // Открываем канал
    let channel = connection.create_channel().await.expect("Channel failed");

    // Объявляем очередь
    let queue = channel
        .queue_declare(
            "messages_queue",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Queue declare failed");

    // Публикуем сообщение
    let payload = b"Hello from lapin to burrowMQ and back!";
    channel
        .basic_publish(
            "",
            "messages_queue",
            BasicPublishOptions::default(),
            payload.as_ref(),
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
            "messages_queue",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Consume failed");

    println!("Waiting for messages...");

    assert_eq!(
        payload,
        consumer
            .next()
            .await
            .expect("Consumer read failed")
            .expect("Queue is empty")
            .data
            .as_slice()
    );

    channel
        .exchange_declare(
            "log",
            ExchangeKind::Direct,
            Default::default(),
            Default::default(),
        )
        .await
        .expect("Exchange declare failed");
    let queue = channel
        .queue_declare(
            "log_queue",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Queue declare failed");
    channel
        .queue_bind(
            "log_queue",
            "log",
            "",
            Default::default(),
            Default::default(),
        )
        .await?;
    channel
        .basic_publish(
            "log",
            "",
            Default::default(),
            b"log message",
            Default::default(),
        )
        .await
        .expect("Publish failed")
        .await
        .expect("Publish confirmation failed");

    let mut consumer = channel
        .basic_consume(
            "log_queue",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Consume failed");

    assert_eq!(
        b"log message",
        consumer
            .next()
            .await
            .expect("Consumer read failed")
            .expect("Queue is empty")
            .data
            .as_slice()
    );

    Ok(())
}
