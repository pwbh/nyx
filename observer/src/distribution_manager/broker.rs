use std::{io::BufReader, net::TcpStream};

use shared_structures::Status;

use super::partition::Partition;

#[derive(Debug)]
pub struct Broker {
    pub id: String,
    pub stream: Option<TcpStream>,
    pub partitions: Vec<Partition>,
    pub reader: Option<BufReader<TcpStream>>,
    pub status: Status,
    pub addr: String,
}

impl Broker {
    pub fn from(id: String, stream: Option<TcpStream>, addr: String) -> Result<Self, String> {
        if let Some(stream) = stream {
            let read_stream = stream.try_clone().map_err(|e| e.to_string())?;
            let reader = BufReader::new(read_stream);

            Ok(Self {
                id,
                partitions: vec![],
                stream: Some(stream),
                reader: Some(reader),
                status: Status::Up,
                addr,
            })
        } else {
            Ok(Self {
                id,
                partitions: vec![],
                stream: None,
                reader: None,
                status: Status::Up,
                addr,
            })
        }
    }

    pub fn restore(&mut self, stream: TcpStream, addr: String) -> Result<(), String> {
        let read_stream = stream.try_clone().map_err(|e| e.to_string())?;
        let reader = BufReader::new(read_stream);

        self.status = Status::Up;
        self.stream = Some(stream);
        self.reader = Some(reader);
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
