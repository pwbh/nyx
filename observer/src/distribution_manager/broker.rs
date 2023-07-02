use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};

use super::{partition::Partition, topic::Topic};

#[derive(Debug)]
pub struct Broker {
    pub stream: TcpStream,
    pub partitions: Vec<Partition>,
}

impl Broker {
    pub fn from(stream: TcpStream) -> Result<Self, String> {
        // spawn_broker_stream_reader(read_stream.clone());
        // spawn_broker_stream_writer(write_stream.clone());

        Ok(Self {
            stream,
            partitions: vec![],
        })
    }
}

// fn spawn_broker_stream_reader(stream: Arc<Mutex<TcpStream>>) -> JoinHandle<()> {
//     std::thread::spawn(move || {
//         let stream = stream.lock().unwrap();
//
//         println!(
//             "Broker read thread spawned for {}",
//             stream.peer_addr().unwrap()
//         );
//
//         let mut buf = String::with_capacity(1024);
//         let mut reader = BufReader::new(&*stream);
//
//         loop {
//             reader.read_line(&mut buf).unwrap();
//             println!("{}", buf);
//             buf.clear();
//         }
//     })
// }
//
// fn spawn_broker_stream_writer(mut stream: Arc<Mutex<TcpStream>>) -> JoinHandle<()> {
//     println!(
//         "Broker write thread spawned for {}",
//         stream.peer_addr().unwrap()
//     );
//
//     std::thread::spawn(move || loop {
//         std::thread::sleep(Duration::from_millis(1500));
//         stream.write("Hello from observer!\n".as_bytes()).unwrap();
//     })
// }
