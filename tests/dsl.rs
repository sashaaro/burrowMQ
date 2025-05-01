// AST (Abstract Syntax Tree) for the AMQP DSL

use std::collections::HashMap;
use futures_lite::StreamExt;
use lapin::message::Delivery;
use lapin::options::{
    BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions, QueuePurgeOptions,
};
use lapin::{
    BasicProperties, Channel, Connection, Consumer, ExchangeKind,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use lapin::types::ChannelId;
use nom::Parser;
use tokio::time::Duration;

use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, one_of, space1},
    multi::separated_list0,
    sequence::{preceded, separated_pair},
};
use tokio::select;

#[derive(Debug, PartialEq)]
pub enum Command {
    ExchangeDeclare {
        name: String,
    },
    QueueDeclare {
        name: String,
    },
    QueueBind {
        queue: String,
        exchange: String,
        // TODO routing_key: Option<String>
    },
    QueuePurge {
        queue: String,
    },
    BasicPublish {
        exchange: Option<String>,
        routing_key: Option<String>,
        body: String,
    },
    ExpectConsume {
        queue: String,
        body: String,
    },
    BasicAck {},
    BasicQos {
        prefetch_count: u16,
        // prefetch_size: u32
    },
}

fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '-')(input)
}

fn quoted_string(input: &str) -> IResult<&str, &str> {
    let (input, _) = one_of("\"'")(input)?;
    let (input, val) = take_while1(|c| c != '"' && c != '\'')(input)?;
    let (input, _) = one_of("\"'")(input)?;
    Ok((input, val))
}

fn key_value(input: &str) -> IResult<&str, (&str, &str)> {
    separated_pair(identifier, char('='), quoted_string).parse(input)
}

fn args_to_map<'a>(args: Vec<(&'a str, &'a str)>) -> std::collections::HashMap<&'a str, &'a str> {
    args.into_iter().collect()
}

fn basic_qos(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("basic.qos")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let prefetch_count = map.get("prefetch_count").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    // let prefetch_size = map.get("prefetch_size").ok_or_else(|| {
    //     nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    // })?;
    Ok((
        input,
        Command::BasicQos {
            prefetch_count: prefetch_count.parse::<u16>().unwrap(),
            // prefetch_size: *prefetch_size as u32,
        },
    ))
}

fn queue_declare(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("queue.declare")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let name = map.get("name").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::QueueDeclare {
            name: name.to_string(),
        },
    ))
}

fn queue_purge(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("queue.purge")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let name = map.get("name").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::QueuePurge {
            queue: name.to_string(),
        },
    ))
}

fn queue_bind(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("queue.bind")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let queue = map.get("queue").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    let exchange = map.get("exchange").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::QueueBind {
            queue: queue.to_string(),
            exchange: exchange.to_string(),
        },
    ))
}

fn queue_exchange(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("exchange.declare")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let name = map.get("name").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::ExchangeDeclare {
            name: name.to_string(),
        },
    ))
}

fn basic_publish(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("basic.publish")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);

    let exchange = map.get("exchange");
    let routing_key = map.get("routing_key");

    if exchange.is_none() && routing_key.is_none() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag,
        )));
    }
    let body = map.get("body").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;

    Ok((
        input,
        Command::BasicPublish {
            exchange: exchange.map(|s| s.to_string()),
            routing_key: routing_key.map(|s| s.to_string()),
            body: body.to_string(),
        },
    ))
}

fn basic_ack(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("basic.ack")(input)?;

    Ok((input, Command::BasicAck {}))
}

fn expect_consume(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("expect.consume")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let queue = map.get("queue").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    let body = map.get("body").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::ExpectConsume {
            queue: queue.to_string(),
            body: body.to_string(),
        },
    ))
}

pub fn parse_command(input: &str) -> IResult<&str, Command> {
    preceded(
        multispace0,
        alt((
            basic_qos,
            queue_exchange,
            queue_declare,
            queue_bind,
            queue_purge,
            basic_publish,
            basic_ack,
            expect_consume,
        )),
    )
    .parse(input)
}

pub fn load_scenario(text: &str) -> Vec<Command> {
    let commands = text
        .trim()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let (_, command) = parse_command(line).expect("failed to parse line");
            command
        })
        .collect();

    commands
}

pub struct Runner<'a> {
    conn: &'a Connection,

    // last_delivery_tag: Option<u64>,
    last_delivery: Option<Delivery>,
    consumer: Option<Consumer>,
    last_delivery_tag: u64,
    before_commands: Vec<Command>,
    
    channels: HashMap<ChannelId, Channel>,
    current_channel_id: Option<ChannelId>
}

impl<'a> Runner<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,

            last_delivery: None,
            consumer: None,
            last_delivery_tag: 0,
            before_commands: vec![],
            channels: HashMap::new(),
            current_channel_id: None
        }
    }

    pub async fn run(&mut self, scenario: &str) {
        self.run_scenario(&load_scenario(scenario)).await;
    }

    pub(crate) fn before(&mut self, before_scenario: &str) {
        self.before_commands = load_scenario(before_scenario)
    }

    pub async fn run_scenario(&mut self, commands: &Vec<Command>) {
        if self.current_channel_id.is_none() {
            let channel = self
                .conn
                .create_channel()
                .await
                .expect("failed to create channel");

            channel.basic_qos(1, BasicQosOptions::default()).await.expect("failed to set qos"); // TODO remove
            
            self.current_channel_id = Some(channel.id()); 
            self.channels.insert(channel.id(), channel);
        };
        let channel = self.channels.get(&self.current_channel_id.unwrap()).unwrap();
        
        for command in self.before_commands.iter().chain(commands.iter()) {
            match command {
                Command::BasicQos { prefetch_count } => {
                    channel
                        .basic_qos(*prefetch_count, BasicQosOptions::default())
                        .await
                        .expect("failed to set qos");
                }
                Command::ExchangeDeclare { name } => {
                    channel
                        .exchange_declare(
                            name,
                            ExchangeKind::Direct,
                            ExchangeDeclareOptions::default(),
                            FieldTable::default(),
                        )
                        .await
                        .expect("failed to declare exchange");
                }
                Command::QueueDeclare { name } => {
                    channel
                        .queue_declare(name, QueueDeclareOptions::default(), FieldTable::default())
                        .await
                        .expect("failed to declare queue");
                }
                Command::QueuePurge { queue } => {
                    channel
                        .queue_purge(queue, QueuePurgeOptions::default())
                        .await
                        .expect("failed to declare purge");
                }
                Command::QueueBind { queue, exchange } => channel
                    .queue_bind(
                        queue,
                        exchange,
                        "",
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .expect("failed to bind queue"),
                Command::BasicPublish {
                    exchange,
                    routing_key,
                    body,
                } => {
                    let mut opts = BasicPublishOptions::default();
                    opts.mandatory = true;
                    let exchange = exchange.clone();
                    let routing_key = routing_key.clone();

                    let exchange = exchange.as_deref().unwrap_or("");
                    let routing_key = routing_key.as_deref().unwrap_or("");

                    let confirm = channel
                        .basic_publish(
                            exchange,
                            routing_key,
                            opts,
                            body.as_bytes(),
                            BasicProperties::default(),
                        )
                        .await
                        .unwrap()
                        .await
                        .unwrap();
                }
                Command::BasicAck {} => {
                    // let delivery = self.last_delivery.as_ref().unwrap();
                    // delivery.ack(BasicAckOptions::default()).await.unwrap();
                    // self.last_delivery = None;
                    channel
                        .basic_ack(self.last_delivery_tag, Default::default())
                        .await
                        .expect("failed to ack");
                }
                Command::ExpectConsume { queue, body } => {
                    let opt = BasicConsumeOptions::default();
                    // opt.no_ack = false;

                    // if self.consumer.is_none() {
                    let mut consumer = channel
                        .basic_consume(queue.as_str(), "test", opt, FieldTable::default())
                        .await
                        .expect("failed to consume");
                    // }

                    let message = select! {
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {
                            None
                        },
                        next = consumer.next() => {
                            let next = next.unwrap();
                            if next.is_ok() {
                                Some(next.unwrap())
                            } else {
                                None
                            }
                        }
                    };

                    assert_ne!(message, None);

                    let message = message.unwrap();
                    // message.ack(BasicAckOptions::default()).await.unwrap();

                    assert_eq!(String::from_utf8_lossy(&message.data), *body);
                    self.last_delivery_tag = message.delivery_tag;
                    self.last_delivery = Some(message);

                    drop(consumer); // basic.Cancel
                    tokio::time::sleep(Duration::from_millis(100)).await; // wait cancel
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_declare() {
        let result = parse_command("queue.declare name=\"my_queue\"");
        assert_eq!(
            result.unwrap().1,
            Command::QueueDeclare {
                name: "my_queue".to_owned()
            }
        );
    }

    #[test]
    fn test_basic_publish() {
        let result = parse_command("basic.publish routing_key=\"my_queue\" body=\"Hello\"");
        assert_eq!(
            result.unwrap().1,
            Command::BasicPublish {
                exchange: None,
                routing_key: Some("my_queue".to_owned()),
                body: "Hello".to_owned(),
            }
        );
    }

    #[test]
    fn test_expect_consume() {
        let result = parse_command("expect.consume queue=\"my_queue\" body=\"Hello\"");
        assert_eq!(
            result.unwrap().1,
            Command::ExpectConsume {
                queue: "my_queue".to_owned(),
                body: "Hello".to_owned(),
            }
        );
    }
}
