use std::sync::Arc;

use shared_structures::Status;
use tokio::sync::Mutex;

use super::partition::Partition;

#[derive(Debug)]
pub struct Broker {
    pub id: String,
    pub partitions: Vec<Partition>,
    pub stream: Option<Arc<Mutex<tokio::io::BufReader<tokio::net::TcpStream>>>>,
    pub status: Status,
    pub addr: String,
}

impl Broker {
    pub fn from(
        id: String,
        stream: Option<tokio::net::TcpStream>,
        addr: String,
    ) -> Result<Self, String> {
        if let Some(stream) = stream {
            let stream = tokio::io::BufReader::new(stream);

            Ok(Self {
                id,
                partitions: vec![],
                stream: Some(Arc::new(Mutex::new(stream))),
                status: Status::Up,
                addr,
            })
        } else {
            Ok(Self {
                id,
                partitions: vec![],
                stream: None,
                status: Status::Up,
                addr,
            })
        }
    }

    pub fn restore(&mut self, stream: tokio::net::TcpStream, addr: String) -> Result<(), String> {
        let stream = tokio::io::BufReader::new(stream);

        self.status = Status::Up;
        self.stream = Some(Arc::new(Mutex::new(stream)));
        self.addr = addr;

        for partition in self.partitions.iter_mut() {
            partition.status = Status::Up
        }

        Ok(())
    }

    pub fn disconnect(&mut self) {
        self.status = Status::Down;
        self.partitions
            .iter_mut()
            .for_each(|p| p.status = Status::Down);
    }

    pub fn get_offline_partitions(&self) -> &[Partition] {
        &self.partitions
    }
}
