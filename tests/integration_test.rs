use burrow_mq::defer;
use burrow_mq::defer::ScopeCall;
use burrow_mq::server;
use env_logger::Builder;
use lapin::{Connection, ConnectionProperties};
use log::LevelFilter;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;

mod dsl;

#[test]
fn main_test() -> anyhow::Result<()> {
    // console_subscriber::init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async { main().await })?;

    Ok(())
}

async fn main() -> anyhow::Result<()> {
    Builder::new()
        .filter(Some("burrow_mq"), LevelFilter::Trace)
        .is_test(true)
        .filter(Some("integration_test"), LevelFilter::Trace)
        .init();

    let no_embedded_amqp = std::env::var("NO_EMBEDDED_AMQP").unwrap_or_default();

    let mut handler: Option<JoinHandle<()>> = None;
    if no_embedded_amqp.is_empty() || no_embedded_amqp == "0" || no_embedded_amqp == "false" {
        handler = Some(tokio::spawn(async {
            let server: server::BurrowMQServer<
                //LockFreeQueue<bytes::Bytes>
                crossbeam_queue::SegQueue<bytes::Bytes>,
            > = server::BurrowMQServer::new();
            server.start_forever(5672).await.expect("Server failed");
        }));

        sleep(Duration::from_millis(100)).await;
    }
    defer!({
        if let Some(handler) = &handler {
            handler.abort();
        }
    });

    let addr = "amqp://127.0.0.1:5672/%2f";
    let connection = Connection::connect(addr, ConnectionProperties::default())
        .await
        .expect("Connection failed");
    log::info!("Connected to RabbitMQ");

    let mut runner = dsl::Runner::new(&connection);

    // publish message to queue via routing key, consume message
    // runner
    //     .run(
    //         r"
    // basic.qos prefetch_count='1'
    // queue.declare name='messages_queue'
    // queue.purge name='messages_queue'
    // basic.publish routing_key='messages_queue' body='hi 1_1'
    // 
    // basic.consume queue='messages_queue' consume_tag='consumer_1'
    // wait 50
    // expect.consumed consume_tag='consumer_1' expect='hi 1_1'
    // basic.ack 1
    // ",
    //     )
    //     .await?;
    // 
    // runner
    //     .run(
    //         r"
    // queue.declare name='messages_queue'
    // queue.purge name='messages_queue'
    // basic.qos prefetch_count='1'
    // basic.consume queue='messages_queue' consume_tag='consumer_1'
    // basic.publish routing_key='messages_queue' body='hi 2_1'
    // basic.publish routing_key='messages_queue' body='hi 2_2'
    // 
    // wait 50
    // basic.ack 1
    // wait 50
    // basic.ack 2
    // 
    // expect.consumed consume_tag='consumer_1' expect='hi 2_1'
    // expect.consumed consume_tag='consumer_1' expect='hi 2_2'
    // ",
    //     )
    //     .await?;

    // publish a message to exchange, consume a message from bound queue with multiple consumers with round-robin
    runner
        .run(
            r"
    exchange.declare name='logs'
    queue.declare name='logs_queue'
    queue.bind queue='logs_queue' exchange='logs'

    #1: basic.consume queue='logs_queue' consume_tag='consumer_1'
    #2: basic.consume queue='logs_queue' consume_tag='consumer_2'
    #3: basic.consume queue='logs_queue' consume_tag='consumer_3'

    #4: basic.publish exchange='logs' body='log 1'
    #4: basic.publish exchange='logs' body='log 22'
    #4: basic.publish exchange='logs' body='log 333'

    #1: basic.ack 1
    #2: basic.ack 1
    #3: basic.ack 1
    wait 200

    #1: expect.consumed expect='log 1'
    #2: expect.consumed expect='log 22'
    #3: expect.consumed expect='log 333'
    ",
        )
        .await?;

    Ok(())
}
