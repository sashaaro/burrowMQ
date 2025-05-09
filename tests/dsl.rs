// AST (Abstract Syntax Tree) for the AMQP DSL

use dashmap::DashMap;
use futures::future::join_all;
use futures_lite::StreamExt;
use lapin::options::{
    BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions, QueuePurgeOptions,
};
use lapin::{
    BasicProperties, Channel, Connection, Consumer, ExchangeKind,
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
};
use nom::Parser;
use regex::Regex;
use std::sync::Arc;
use tokio::time::Duration;

use nom::character::complete::{digit0, u64};
use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char, multispace0, one_of, space1},
    multi::separated_list0,
    sequence::{preceded, separated_pair},
};
use tokio::select;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

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
    Consume {
        queue: String,
        consume_tag: String,
    },
    ExpectConsumed {
        expect: String,
    },
    BasicAck {
        delivery_tag: u64,
    },
    BasicQos {
        prefetch_count: u16,
        // prefetch_size: u32
    },
    Wait {
        milliseconds: u64,
    },
}

// Helper struct to pair a command with an optional channel id
#[derive(Debug)]
pub struct ScenarioCommand {
    pub channel_id: Option<usize>,
    pub command: Command,
}

fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '-')(input)
}

fn quoted_string(input: &str) -> IResult<&str, &str> {
    let (input, _) = one_of("\"'[")(input)?;
    let (input, val) = take_while1(|c| c != '"' && c != '\'' && c != ']')(input)?;
    let (input, _) = one_of("\"']")(input)?;
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

fn wait(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("wait")(input)?;
    let (input, _) = space1(input)?;
    let (input, milliseconds) = digit0(input)?;
    Ok((
        input,
        Command::Wait {
            milliseconds: milliseconds.parse().unwrap(),
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
    let (input, _) = space1(input)?;
    let (input, delivery_tag) = u64(input)?;

    Ok((input, Command::BasicAck { delivery_tag }))
}

fn basic_consume(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("basic.consume")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let queue = map.get("queue").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    let consume_tag = map.get("consume_tag").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;
    Ok((
        input,
        Command::Consume {
            queue: queue.to_string(),
            consume_tag: consume_tag.to_string(),
        },
    ))
}

fn expect_consumed(input: &str) -> IResult<&str, Command> {
    let (input, _) = tag("expect.consumed")(input)?;
    let (input, _) = space1(input)?;
    let (input, args) = separated_list0(space1, key_value).parse(input)?;
    let map = args_to_map(args);
    let expect = map.get("expect").ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;

    Ok((
        input,
        Command::ExpectConsumed {
            expect: expect.to_string(),
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
            basic_consume,
            expect_consumed,
            wait,
        )),
    )
    .parse(input)
}

// Parses a scenario line, extracting optional channel id and the command
fn parse_scenario_line(line: &str) -> ScenarioCommand {
    // Regex to match e.g. "#2: ..."
    let re = Regex::new(r"^#(\d+):\s*(.*)").unwrap();
    if let Some(caps) = re.captures(line) {
        let channel_id = caps.get(1).unwrap().as_str().parse::<u16>().unwrap();
        let command_str = caps.get(2).unwrap().as_str();
        let (_, command) = parse_command(command_str).expect("failed to parse line");
        ScenarioCommand {
            channel_id: Some(channel_id.into()),
            command,
        }
    } else {
        let (_, command) = parse_command(line).expect("failed to parse line");
        ScenarioCommand {
            channel_id: None,
            command,
        }
    }
}

// Loads scenario lines as ScenarioCommand with optional channel id
pub fn load_scenario(text: &str) -> Vec<ScenarioCommand> {
    text.trim()
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(parse_scenario_line)
        .collect()
}

pub struct Runner<'a> {
    conn: &'a Connection,
    // before_commands: Vec<Command>,
    channels: Vec<Channel>,
    consumers: Arc<DashMap<String, Consumer>>,
    deliveries: Arc<Mutex<Vec<String>>>,

    current_channel_id: usize,
    handlers: Vec<JoinHandle<()>>,
}

impl<'a> Runner<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,

            consumers: Default::default(),
            deliveries: Default::default(),
            // before_commands: Default::default(),
            channels: Default::default(),
            current_channel_id: 0,
            handlers: Default::default(),
        }
    }

    pub async fn run(&mut self, scenario: &str) -> anyhow::Result<()> {
        self.run_scenario(&load_scenario(scenario)).await?;
        Ok(())
    }

    pub async fn run_command(
        &mut self,
        scenario_command: &ScenarioCommand,
        token: CancellationToken,
    ) -> anyhow::Result<Receiver<()>> {
        let (done_tx, done_rx) = oneshot::channel();

        self.current_channel_id = scenario_command
            .channel_id
            .unwrap_or(self.current_channel_id);
        if self.channels.get(self.current_channel_id).is_none() {
            // If no channel exists, create a default one
            let channel = self.conn.create_channel().await?;
            channel.basic_qos(1, BasicQosOptions::default()).await?;
            self.channels.insert(self.current_channel_id, channel);
        }

        let channel = self.channels.get(self.current_channel_id).unwrap();
        let command = &scenario_command.command;

        log::debug!("command: {command:?}");
        match command {
            Command::Wait { milliseconds } => {
                tokio::time::sleep(Duration::from_millis(*milliseconds)).await;
                done_tx.send(()).unwrap();
            }
            Command::BasicQos { prefetch_count } => {
                channel
                    .basic_qos(*prefetch_count, BasicQosOptions::default())
                    .await?;
                done_tx.send(()).unwrap();
            }
            Command::ExchangeDeclare { name } => {
                channel
                    .exchange_declare(
                        name,
                        ExchangeKind::Direct,
                        ExchangeDeclareOptions::default(),
                        FieldTable::default(),
                    )
                    .await?;
                done_tx.send(()).unwrap();
            }
            Command::QueueDeclare { name } => {
                channel
                    .queue_declare(name, QueueDeclareOptions::default(), FieldTable::default())
                    .await?;
                done_tx.send(()).unwrap();
            }
            Command::QueuePurge { queue } => {
                channel
                    .queue_purge(queue, QueuePurgeOptions::default())
                    .await?;
                done_tx.send(()).unwrap();
            }
            Command::QueueBind { queue, exchange } => {
                channel
                    .queue_bind(
                        queue,
                        exchange,
                        "",
                        QueueBindOptions::default(),
                        FieldTable::default(),
                    )
                    .await?;
                done_tx.send(()).unwrap();
            }
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
                    .await?
                    .await?;
                done_tx.send(()).unwrap();
            }
            Command::ExpectConsumed { expect } => {
                tokio::time::sleep(Duration::from_millis(100)).await;

                let mut deliveries = self.deliveries.lock().await;

                let idx = deliveries.iter().position(|v| v == expect);
                assert_ne!(idx, None);

                deliveries.remove(idx.unwrap());
                drop(deliveries);
                done_tx.send(()).unwrap();
            }
            Command::BasicAck { delivery_tag } => {
                channel.basic_ack(*delivery_tag, Default::default()).await?;
                done_tx.send(()).unwrap();
            }
            Command::Consume { queue, consume_tag } => {
                let opt = BasicConsumeOptions::default();
                let consumer = channel
                    .basic_consume(queue.as_str(), consume_tag, opt, FieldTable::default())
                    .await?;

                self.consumers.insert(consume_tag.clone(), consumer);

                let consumers = Arc::clone(&self.consumers);
                let deliveries = Arc::clone(&self.deliveries);

                let consume_tag = consume_tag.clone();
                tokio::spawn(async move {
                    let deliveries = Arc::clone(&deliveries);
                    let consumers = Arc::clone(&consumers);

                    log::debug!("start loop!!");

                    loop {
                        let consumers = Arc::clone(&consumers);
                        let token = token.clone();

                        let mut consume = consumers.get_mut(&consume_tag).unwrap();

                        select! {
                            _ = token.cancelled() => {

                                        log::debug!("cancelled!!");
                                break;
                            },
                            delivery = consume.next() => {
                                let Some(delivery) = delivery else {
                                    break;
                                };
                                let Ok(delivery) = delivery else {
                                    log::warn!("delivery error: {delivery:?}");
                                    break;
                                };
                                deliveries
                                    .lock()
                                    .await
                                    .push(String::from_utf8(delivery.data).unwrap());
                            }
                        };
                    }

                    done_tx.send(()).unwrap();
                });
            }
        };

        Ok(done_rx)
    }

    pub async fn run_scenario(&mut self, commands: &Vec<ScenarioCommand>) -> anyhow::Result<()> {
        let cancel_token = CancellationToken::new();

        let mut done = vec![];
        for (line, scenario_command) in commands.iter().enumerate() {
            match self
                .run_command(scenario_command, cancel_token.clone())
                .await
            {
                Ok(d) => done.push(d),
                Err(err) => {
                    cancel_token.cancel();
                    return Err(anyhow::anyhow!("Failed to run command {}: {err}", line));
                }
            }
        }

        self.deliveries.lock().await.clear();
        cancel_token.cancel();
        join_all(done).await;

        self.consumers.clear();

        self.channels.clear();
        self.current_channel_id = 0;

        Ok(())
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
    fn test_expect_consumed() {
        let result = parse_command(r"expect.consumed expect='msg'");
        assert_eq!(
            result.unwrap().1,
            Command::ExpectConsumed {
                expect: "msg".to_owned(),
            }
        );
    }
}
