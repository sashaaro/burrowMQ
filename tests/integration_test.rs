use burrowMQ::server;
use futures_lite::stream::StreamExt;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};

#[tokio::test]
async fn test_connect() -> anyhow::Result<()> {
    tokio::spawn(async {
        let server = server::BurrowMQServer::new();
        server.start_forever().await.expect("Server failed");
    });

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

    //channel.exchange_declare("common", ExchangeKind::Direct);

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

    Ok(())
}
