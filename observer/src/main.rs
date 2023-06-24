use std::{
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    thread::JoinHandle,
};

mod command_processor;
mod utils;

use command_processor::CommandProcessor;

fn main() {
    let mut command_processor = CommandProcessor::new();

    // Open a TCP stream for brokers to connect to
    let listener = TcpListener::bind("localhost:3000").unwrap();
    println!("Observer is ready to accept brokers on port 3000");
    std::thread::spawn(move || loop {
        let stream = listener.incoming().next();

        if let Some(stream) = stream {
            match stream {
                Ok(stream) => {
                    println!("Broker connection occured: {}", stream.peer_addr().unwrap());
                    spaw_broker_stream_thread(stream);
                }
                Err(e) => println!("Failed to establish connection: {}", e),
            }
        }
    });

    // This will make sure our main thread will never exit until the user will issue an EXIT command by himself
    loop {
        match command_processor.process_raw_command() {
            Ok(status) => println!("\x1b[32m✓ {}\x1b[0m", status),
            Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
        };
    }
}

fn spaw_broker_stream_thread(stream: TcpStream) -> JoinHandle<()> {
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
