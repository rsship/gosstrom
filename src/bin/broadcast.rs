use anyhow::Context;
use rand::prelude::*;
use serde::de::value::UsizeDeserializer;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{StderrLock, StdoutLock, Write};
use std::time::Duration;
use strom::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    InitOk,
    Init(Init),
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        seen: HashSet<usize>,
    },
}

enum InjectedPayload {
    Gossip,
}

#[derive(Debug, Clone)]
struct BroadcastNode {
    node_id: String,
    msg_id: usize,
    messages: Vec<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

impl Node<BroadcastPayload, InjectedPayload> for BroadcastNode {
    fn step(
        &mut self,
        input: Event<BroadcastPayload, InjectedPayload>,
        output: &mut StdoutLock,
        inject: std::sync::mpsc::Sender<Event<BroadcastPayload, InjectedPayload>>,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(msg) => {
                match msg.body.payload {
                    BroadcastPayload::Init(init) => {
                        std::thread::spawn(move || loop {
                            std::thread::sleep(Duration::from_millis(300));
                            if let Err(_) = inject.send(Event::Injected(InjectedPayload::Gossip)) {
                                break;
                            }
                        });
                        let reply = Message::<BroadcastPayload> {
                            src: msg.dest,
                            dest: msg.src,
                            body: Body {
                                msg_id: Some(self.msg_id),
                                in_reply_to: msg.body.msg_id,
                                payload: BroadcastPayload::InitOk,
                            },
                        };
                        serde_json::to_writer(&mut *output, &reply)
                            .context("could not serialize to STDOUT")?;
                        output
                            .write_all(b"\n")
                            .context("could not new line to STDOUT")?;

                        self.known = init
                            .node_ids
                            .into_iter()
                            .map(|nid| (nid, HashSet::new()))
                            .collect();

                        self.neighborhood = Vec::new();
                        self.node_id = init.node_id;
                        self.msg_id += 1;
                    }
                    BroadcastPayload::InitOk => {}
                    BroadcastPayload::Broadcast { message } => {
                        self.messages.push(message);
                        self.known.get_mut(&self.node_id).unwrap().insert(message);
                        let reply = Message::<BroadcastPayload> {
                            src: msg.dest,
                            dest: msg.src,
                            body: Body {
                                msg_id: Some(self.msg_id),
                                in_reply_to: msg.body.msg_id,
                                payload: BroadcastPayload::BroadcastOk,
                            },
                        };

                        serde_json::to_writer(&mut *output, &reply)
                            .context("could not serialize to STDOUT")?;

                        output
                            .write_all(b"\n")
                            .context("could not write new line to STDOUT")?;

                        self.msg_id += 1;
                    }
                    BroadcastPayload::BroadcastOk => {}
                    BroadcastPayload::Read => {
                        let reply = Message::<BroadcastPayload> {
                            src: msg.dest,
                            dest: msg.src,
                            body: Body {
                                msg_id: Some(self.msg_id),
                                in_reply_to: msg.body.msg_id,
                                payload: BroadcastPayload::ReadOk {
                                    messages: self.messages.clone(),
                                },
                            },
                        };

                        serde_json::to_writer(&mut *output, &reply)
                            .context("could not serialize to STDOUT")?;

                        output
                            .write_all(b"\n")
                            .context("could not write new line to STDOUT")?;

                        self.msg_id += 1;
                    }
                    BroadcastPayload::ReadOk { messages } => {}
                    BroadcastPayload::Topology { mut topology } => {
                        self.neighborhood = topology
                            .remove(&self.node_id)
                            .context("Could find current node in node_ids")?;

                        let reply = Message::<BroadcastPayload> {
                            src: msg.dest,
                            dest: msg.src,
                            body: Body {
                                msg_id: Some(self.msg_id),
                                in_reply_to: msg.body.msg_id,
                                payload: BroadcastPayload::TopologyOk,
                            },
                        };

                        serde_json::to_writer(&mut *output, &reply)
                            .context("could not serialize to STDOUT")?;

                        output
                            .write_all(b"\n")
                            .context("could not write new line to STDOUT")?;
                    }
                    BroadcastPayload::TopologyOk => {}
                    BroadcastPayload::Gossip { seen } => {
                        self.known
                            .get_mut(&msg.dest)
                            .unwrap()
                            .extend(seen.iter().copied());
                        self.messages.extend(seen);
                    }
                }

                Ok(())
            }
            Event::Injected(payload) => match payload {
                InjectedPayload::Gossip => {
                    for n in &self.neighborhood {
                        let known = &self.known[n];

                        let (mut will_send, already_known): (HashSet<usize>, HashSet<usize>) = self
                            .messages
                            .iter()
                            .copied()
                            .partition(|m| !known.contains(m));

                        let mut rng = rand::thread_rng();

                        will_send.extend(already_known.iter().filter(|_| {
                            rng.gen_ratio(
                                10.min(self.messages.len() as u32),
                                self.messages.len() as u32,
                            )
                        }));

                        let reply = Message {
                            src: self.node_id.clone(),
                            dest: n.to_owned(),
                            body: Body {
                                msg_id: None,
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip {
                                    seen: will_send,
                                },
                            },
                        };

                        serde_json::to_writer(&mut *output, &reply)
                            .context("could not serialize to STDOUT")?;

                        output
                            .write_all(b"\n")
                            .context("could not write new line to STDOUT")?;
                    }
                    Ok(())
                }
            },
        }
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastPayload, InjectedPayload>(BroadcastNode {
        msg_id: 0,
        node_id: "".to_string(),
        messages: Vec::new(),
        known: HashMap::new(),
        neighborhood: Vec::new(),
    })?;
    Ok(())
}
