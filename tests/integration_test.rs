use burrow_mq::server;
use lapin::{Connection, ConnectionProperties};
use std::time::Duration;
use tokio::time::sleep;

mod dsl;

#[tokio::test]
async fn main_test() -> anyhow::Result<()> {
    // console_subscriber::init();

    let no_embedded_amqp = std::env::var("NO_EMBEDDED_AMQP").unwrap_or_default();
    if no_embedded_amqp == "" || no_embedded_amqp == "0" || no_embedded_amqp == "false" {
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

    let mut runner = dsl::Runner::new(&connection);

    //     runner.before(r"
    // basic.qos prefetch_count='1'
    // ",
    //     );

    // publish message to queue via routing key, consume message
    runner
        .run(
            r"
    queue.declare name='messages_queue'
    queue.purge name='messages_queue'
    basic.publish routing_key='messages_queue' body='HELLO FROM LAPIN!'
    
    expect.consume queue='messages_queue' body='HELLO FROM LAPIN!'
        ",
        )
        .await;

    runner
        .run(
            r"
#0: queue.declare name='messages_queue'
#0: queue.purge name='messages_queue'
#0: basic.consume queue='messages_queue' consume_tag='first_consumer'
#0: basic.publish routing_key='messages_queue' body='NEW MESSAGE FROM LAPIN!'
#0: basic.publish routing_key='messages_queue' body='MESSAGE #2 FROM LAPIN!'

expect.consumed consume_tag='first_consumer' expect='NEW MESSAGE FROM LAPIN!'
#0: basic.ack 1
",
            //expect.consumed consume_tag='first_consumer' expect='MESSAGE #2 FROM LAPIN!'

            // expect.consume queue='messages_queue' body='MESSAGE #2 FROM LAPIN!'
        )
        .await;

    // publish a message to exchange, consume a message from bound queue
    runner
        .run(
            r"
exchange.declare name='logs'
queue.declare name='logs_queue'
queue.bind queue='logs_queue' exchange='logs'
basic.publish exchange='logs' body='log message'

expect.consume queue='logs_queue' body='log message'
basic.ack 1
",
        )
        .await;

    Ok(())
}
