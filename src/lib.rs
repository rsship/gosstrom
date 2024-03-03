use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::BufRead;
use std::io::StdoutLock;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    pub dest: String,
    pub body: Body<Payload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Event<Payload, InjectedPayload = ()> {
    Message(Message<Payload>),
    Injected(InjectedPayload),
}

pub trait Node<Payload, InjectedPayload = ()> {
    fn step(
        &mut self,
        msg: Event<Payload, InjectedPayload>,
        output: &mut StdoutLock,
        inject: std::sync::mpsc::Sender<Event<Payload, InjectedPayload>>,
    ) -> anyhow::Result<()>;
}

pub fn main_loop<S, P, IP>(mut state: S) -> anyhow::Result<()>
where
    P: DeserializeOwned + Send + 'static,
    IP: Send + 'static,
    S: Node<P, IP>,
{
    let (tx, rx) = std::sync::mpsc::channel();
    let mut stdout = std::io::stdout().lock();

    let thread_tx = tx.clone();
    let join = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();
        for line in stdin.lines() {
            let msg = line.context("Could not read STDIN line")?;
            let input: Message<P> = serde_json::from_str(&msg)
                .unwrap_or_else(|_| panic!("input from STDIN could no be deserialized"));
            if let Err(_) = thread_tx.send(Event::Message(input)) {
                return Ok::<(), anyhow::Error>(());
            }
        }
        Ok(())
    });

    for input in rx {
        state
            .step(input, &mut stdout, tx.clone())
            .context("Node step function failed!")?;
    }
    join.join()
        .expect("stdin thread panicked ")
        .context("stdin thread err ")?;
    Ok(())
}
