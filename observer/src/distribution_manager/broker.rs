use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    thread::JoinHandle,
};

use super::partition::Partition;

pub const ID_FIELD_CHAR_COUNT: usize = 36;

#[derive(Debug)]
pub struct Broker {
    pub id: String,
    pub stream: TcpStream,
    pub partitions: Vec<Partition>,
    pub reader: BufReader<TcpStream>,
}

impl Broker {
    pub fn from(metadata: (String, TcpStream, BufReader<TcpStream>)) -> Result<Self, String> {
        Ok(Self {
            id: metadata.0,
            partitions: vec![],
            stream: metadata.1,
            reader: metadata.2,
        })
    }
}

fn spawn_broker_stream_reader(stream: TcpStream) -> JoinHandle<()> {
    std::thread::spawn(move || {
        println!(
            "Broker read thread spawned for {}",
            stream.peer_addr().unwrap()
        );

        let mut buf = String::with_capacity(1024);
        let mut reader = BufReader::new(&stream);

        loop {
            let data_size = reader.read_line(&mut buf).unwrap();
            if data_size == 0 {
                println!(
                    "Connection with {} has been closed.",
                    stream.peer_addr().unwrap()
                );
                break;
            }
            println!("read: {}", buf);
            buf.clear();
        }
    })
}
