use std::{io::Write, net::TcpStream};

use partition::Partition;

use uuid::Uuid;

mod partition;
mod topic;

#[derive(Debug)]
pub struct Broker {
    id: String,
    partitions: Vec<Partition>,
    pub observer_stream: TcpStream,
}

impl Broker {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            partitions: vec![],
            observer_stream: stream,
        }
    }

    pub fn send_info(&mut self) -> std::io::Result<usize> {
        let payload = format!("{}\n", self.id);
        self.observer_stream.write(payload.as_bytes())
    }
}
