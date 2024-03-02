use anyhow::Context;
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
}

#[derive(Debug, Clone)]
struct BroadcastNode {
    id: usize,
    messages: Vec<usize>,
    known: HashMap<String, HashSet<usize>>,
    neighborhood: Vec<String>,
}

impl<InjectedPayload> Node<BroadcastPayload, InjectedPayload> for BroadcastNode {
    fn step(
        &mut self,
        input: Event<BroadcastPayload, InjectedPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                match input.body.payload {
                    BroadcastPayload::Init(init) => {
                        todo!()
                    }
                    BroadcastPayload::InitOk => {
                        todo!()
                    }
                    BroadcastPayload::Broadcast { message } => {
                        todo!()
                    }
                    BroadcastPayload::BroadcastOk => {
                        todo!()
                    }
                    BroadcastPayload::Read => {
                        todo!()
                    }
                    BroadcastPayload::ReadOk { messages } => {
                        todo!()
                    }
                    BroadcastPayload::Topology { topology } => {
                        todo!()
                    }
                    BroadcastPayload::TopologyOk => {
                        todo!()
                    }
                }

                Ok(())
            }
            Event::Injected(payload) => {
                todo!();
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastPayload, ()>(BroadcastNode {
        id: 0,
        messages: Vec::new(),
        known: HashMap::new(),
        neighborhood: Vec::new(),
    })?;
    Ok(())
}
