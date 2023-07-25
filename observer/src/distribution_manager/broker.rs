use std::{io::BufReader, net::TcpStream};

use shared_structures::Status;

use super::partition::Partition;

#[derive(Debug)]
pub struct Broker {
    pub id: String,
    pub stream: TcpStream,
    pub partitions: Vec<Partition>,
    pub reader: BufReader<TcpStream>,
    pub status: Status,
}

impl Broker {
    pub fn from(id: String, stream: TcpStream) -> Result<Self, String> {
        let read_stream = stream.try_clone().map_err(|e| e.to_string())?;
        let reader = BufReader::new(read_stream);

        Ok(Self {
            id,
            partitions: vec![],
            stream,
            reader,
            status: Status::Up,
        })
    }

    pub fn restore(&mut self, stream: TcpStream) -> Result<(), String> {
        let read_stream = stream.try_clone().map_err(|e| e.to_string())?;
        let reader = BufReader::new(read_stream);

        self.status = Status::Up;
        self.stream = stream;
        self.reader = reader;

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
