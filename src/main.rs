use std::{
    collections::HashMap,
    io::{stdout, StdinLock, StdoutLock, Write},
    process::id,
    usize,
};

use anyhow::{bail, Context};
use serde::{de::value::UsizeDeserializer, Deserialize, Serialize};
use serde_json;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body {
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: String,
    },
    BroadcastOk,
    Broadcast {
        message: usize,
    },
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct Node {
    id: usize,
}

impl Node {
    pub fn step(
        &mut self,
        msg: Message,
        output: &mut StdoutLock,
        msg_store: &mut Vec<usize>,
    ) -> anyhow::Result<()> {
        match msg.body.payload {
            Payload::Echo { echo } => {
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::EchoOk { echo },
                    },
                };

                serde_json::to_writer::<&mut StdoutLock, Message>(output, &reply)
                    .context("Could not Write to stdout")?;
            }
            Payload::InitOk => bail!("We should not be receive this event type"),
            Payload::Init { .. } => {
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::InitOk,
                    },
                };

                serde_json::to_writer::<&mut StdoutLock, Message>(output, &reply)
                    .context("Could not Write to stdout")?;
            }
            Payload::Generate => {
                let guid = format!("{}-{}", self.id, msg.dest);
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::GenerateOk { id: guid },
                    },
                };

                serde_json::to_writer::<&mut StdoutLock, Message>(output, &reply)
                    .context("Could not Write to stdout")?;
            }
            Payload::Broadcast { message } => {
                msg_store.push(message);
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::BroadcastOk,
                    },
                };

                serde_json::to_writer::<&mut StdoutLock, Message>(output, &reply)
                    .context("Could not Write to stdout")?;
            }
            Payload::Read => {
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::ReadOk {
                            messages: msg_store.clone(),
                        },
                    },
                };

                serde_json::to_writer::<&mut StdoutLock, Message>(output, &reply)
                    .context("Could not Write to stdout")?;
            }

            Payload::Topology { .. } => {
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Payload::TopologyOk,
                    },
                };
                serde_json::to_writer::<&mut StdoutLock, Message>(output, &reply)
                    .context("Could not Write to stdout")?;
            }
            Payload::TopologyOk => {}
            Payload::BroadcastOk => {}
            Payload::EchoOk { .. } => {}
            Payload::GenerateOk { .. } => {}
            Payload::ReadOk { .. } => {}
        }

        output
            .write_all(b"\n")
            .context("could not add new line to STDOUT")?;
        self.id += 1;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();

    let mut node = Node { id: 0 };
    let mut msg_store = Vec::new();
    for input in inputs {
        let input = input.context("Mealstrom input from STDIN could not be serialized")?;
        node.step(input, &mut stdout, &mut msg_store)
            .context("Node step function failed")?;
    }

    Ok(())
}
