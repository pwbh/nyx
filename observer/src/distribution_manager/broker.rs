use std::{
    io::{BufRead, BufReader, Write},
    net::TcpStream,
    thread::JoinHandle,
    time::Duration,
};

pub struct Broker {
    read_handle: JoinHandle<()>,
    write_handle: JoinHandle<()>,
}

impl Broker {
    pub fn from(stream: TcpStream) -> Result<Self, String> {
        let read_stream = match stream.try_clone() {
            Ok(stream) => stream,
            Err(e) => return Err(e.to_string()),
        };

        Ok(Self {
            read_handle: spawn_broker_stream_reader(read_stream),
            write_handle: spawn_broker_stream_writer(stream),
        })
    }
}

fn spawn_broker_stream_reader(stream: TcpStream) -> JoinHandle<()> {
    println!(
        "Broker read thread spawned for {}",
        stream.peer_addr().unwrap()
    );

    std::thread::spawn(move || {
        let mut buf = String::with_capacity(1024);
        let mut reader = BufReader::new(&stream);

        loop {
            reader.read_line(&mut buf).unwrap();
            println!("{}", buf);
            buf.clear();
        }
    })
}

fn spawn_broker_stream_writer(mut stream: TcpStream) -> JoinHandle<()> {
    println!(
        "Broker write thread spawned for {}",
        stream.peer_addr().unwrap()
    );

    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(1500));
        stream.write("Hello from observer!\n".as_bytes()).unwrap();
    })
}
