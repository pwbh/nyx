use std::{io::BufReader, net::TcpStream};

use shared_structures::Status;

use super::partition::Partition;

pub const ID_FIELD_CHAR_COUNT: usize = 36;

#[derive(Debug)]
pub struct Broker {
    pub id: String,
    pub stream: TcpStream,
    pub partitions: Vec<Partition>,
    pub reader: BufReader<TcpStream>,
    pub status: Status,
}

impl Broker {
    pub fn from(metadata: (String, TcpStream, BufReader<TcpStream>)) -> Result<Self, String> {
        Ok(Self {
            id: metadata.0,
            partitions: vec![],
            stream: metadata.1,
            reader: metadata.2,
            status: Status::Up,
        })
    }

    pub fn restore(&mut self, metadata: (String, TcpStream, BufReader<TcpStream>)) {
        self.status = Status::Up;
        self.stream = metadata.1;
        self.reader = metadata.2;

        for partition in self.partitions.iter_mut() {
            partition.status = Status::Up
        }
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
