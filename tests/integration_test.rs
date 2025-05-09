use burrow_mq::server;
use env_logger::Builder;
use lapin::{Connection, ConnectionProperties};
use log::LevelFilter;
use std::time::Duration;
use tokio::time::sleep;

mod dsl;

#[tokio::test]
async fn main_test() -> anyhow::Result<()> {
    // console_subscriber::init();

    Builder::new()
        .filter(Some("burrow_mq"), LevelFilter::Trace)
        .is_test(true)
        .filter(Some("integration_test"), LevelFilter::Trace)
        .init();

    let mut handlers = vec![];
    let no_embedded_amqp = std::env::var("NO_EMBEDDED_AMQP").unwrap_or_default();
    if no_embedded_amqp.is_empty() || no_embedded_amqp == "0" || no_embedded_amqp == "false" {
        let handler = tokio::spawn(async {
            let server: server::BurrowMQServer<
                //LockFreeQueue<bytes::Bytes>
                crossbeam_queue::SegQueue<bytes::Bytes>,
            > = server::BurrowMQServer::new();
            server.start_forever(5672).await.expect("Server failed");
        });
        handlers.push(handler);
        sleep(Duration::from_millis(100)).await;
    }

    let addr = "amqp://127.0.0.1:5672/%2f";
    let connection = Connection::connect(addr, ConnectionProperties::default())
        .await
        .expect("Connection failed");
    log::info!("Connected to RabbitMQ");

    let mut runner = dsl::Runner::new(&connection);

    // publish message to queue via routing key, consume message
    runner
        .run(
            r"
    basic.qos prefetch_count='1'
    queue.declare name='messages_queue'
    queue.purge name='messages_queue'
    basic.publish routing_key='messages_queue' body='hi 1_1'
    
    basic.consume queue='messages_queue' consume_tag='consumer_1'
    expect.consumed consume_tag='consumer_1' expect='hi 1_1'
    basic.ack 1
    ",
        )
        .await
        .map_err(|err| {
            handlers.iter().for_each(|h| h.abort());
            err
        })?;

    runner
        .run(
            r"
    queue.declare name='messages_queue'
    queue.purge name='messages_queue'
    basic.qos prefetch_count='1'
    basic.consume queue='messages_queue' consume_tag='consumer_1'
    basic.publish routing_key='messages_queue' body='hi 2_1'
    basic.publish routing_key='messages_queue' body='hi 2_2'
    
    wait 50
    basic.ack 1
    wait 50
    basic.ack 2
    
    expect.consumed consume_tag='consumer_1' expect='hi 2_1'
    expect.consumed consume_tag='consumer_1' expect='hi 2_2'
    ",
        )
        .await?;

    // publish a message to exchange, consume a message from bound queue with multiple consumers with round-robin
    runner
        .run(
            r"
    exchange.declare name='logs'
    queue.declare name='logs_queue'
    queue.bind queue='logs_queue' exchange='logs'

    #0: basic.consume queue='logs_queue' consume_tag='consumer_1'
    #1: basic.consume queue='logs_queue' consume_tag='consumer_2'
    #2: basic.consume queue='logs_queue' consume_tag='consumer_3'

    #3: basic.publish exchange='logs' body='log 3_1'
    #3: basic.publish exchange='logs' body='log 3_2'
    #3: basic.publish exchange='logs' body='log 3_3'

    #0: basic.ack 1
    #1: basic.ack 1
    #2: basic.ack 1
    wait 200

    #1: expect.consumed expect='log 3_1'
    #2: expect.consumed expect='log 3_2'
    #3: expect.consumed expect='log 3_3'
    ",
        )
        .await?;

    Ok(())
}
