// AST (Abstract Syntax Tree) for the AMQP DSL

use futures_lite::future::block_on;
use futures_lite::StreamExt;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind, options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions}, types::FieldTable, Consumer};
use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use nom::Parser;
use tokio::time::Duration;

#[derive(Debug, PartialEq)]
pub enum Command {
    QueueDeclare { name: String },
    BasicPublish { routing_key: String, body: String },
    ExpectConsume { queue: String, body: String },
    BasicAck {},
}

// Parser using nom
use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, none_of, one_of, space1},
    combinator::{map, map_res},
    multi::separated_list0,
    sequence::{preceded, separated_pair, tuple},
};
use tokio::select;

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

fn basic_publish(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("basic.publish")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let routing_key = map.get("routing_key").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    let body = map.get("body").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::BasicPublish {
            routing_key: routing_key.to_string(),
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
        alt((queue_declare, basic_publish, basic_ack, expect_consume)),
    )
    .parse(input)
}

pub fn load_scenario(text: &str) -> Scenario {
    let commands = text
        .trim()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let (_, command) = parse_command(line).expect("failed to parse line");
            command
        })
        .collect();

    Scenario {
        commands,
        last_delivery: None,
        last_delivery_tag: 0,
        consumer: None,
    }
}

pub struct Scenario {
    commands: Vec<Command>,

    // last_delivery_tag: Option<u64>,
    last_delivery: Option<Delivery>,
    consumer: Option<Consumer>,
    last_delivery_tag: u64
}

impl Scenario {
    pub async fn run(&mut self, channel: &Channel) {
        for command in &self.commands {
            match command {
                Command::QueueDeclare { name } => {
                    channel
                        .queue_declare(name, QueueDeclareOptions::default(), FieldTable::default())
                        .await
                        .unwrap();
                }
                Command::BasicPublish { routing_key, body } => {
                    let mut opts = BasicPublishOptions::default();
                    opts.mandatory = true;

                    let _ = channel
                        .basic_publish(
                            "",
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
                    let _ = channel
                        .basic_ack(self.last_delivery_tag.into(), Default::default())
                        .await
                        .expect("failed to ack");
                }
                Command::ExpectConsume { queue, body } => {
                    let mut opt = BasicConsumeOptions::default();
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
                name: "my_queue".into()
            }
        );
    }

    #[test]
    fn test_basic_publish() {
        let result = parse_command("basic.publish routing_key=\"my_queue\" body=\"Hello\"");
        assert_eq!(
            result.unwrap().1,
            Command::BasicPublish {
                routing_key: "my_queue".into(),
                body: "Hello".into(),
            }
        );
    }

    #[test]
    fn test_expect_consume() {
        let result = parse_command("expect.consume queue=\"my_queue\" body=\"Hello\"");
        assert_eq!(
            result.unwrap().1,
            Command::ExpectConsume {
                queue: "my_queue".into(),
                body: "Hello".into(),
            }
        );
    }
}
