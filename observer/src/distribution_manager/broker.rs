use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpStream,
    thread::JoinHandle,
};

use super::partition::Partition;

const ID_FIELD_CHAR_COUNT: usize = 36;

#[derive(Debug)]
pub struct Broker {
    pub id: String,
    pub stream: TcpStream,
    pub partitions: Vec<Partition>,
    reader: BufReader<TcpStream>,
}

impl Broker {
    pub fn from(stream: TcpStream) -> Result<Self, String> {
        let read_stream = stream.try_clone().map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(read_stream);

        let mut id: String = String::with_capacity(ID_FIELD_CHAR_COUNT);
        reader.read_line(&mut id).map_err(|e| e.to_string())?;

        Ok(Self {
            id: id.trim().to_string(),
            partitions: vec![],
            stream,
            reader,
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
