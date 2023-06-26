use clap::{arg, command};
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    thread::JoinHandle,
    time::Duration,
};

mod command_processor;
mod utils;

use command_processor::CommandProcessor;

fn main() {
    let matches = command!().arg(
        arg!(-f --follow <HOST> "Runs the Observer as a follower for leader located at <HOST>, Host MUST by booted without -f flag.")
        .required(false)
    ).get_matches();

    match matches.get_one::<String>("follow") {
        Some(host) => println!("Booting as a follower for {}", host),
        None => println!("Bootin as a leader"),
    };

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
                    spawn_broker_stream_reader(stream.try_clone().unwrap());
                    spawn_broker_stream_writer(stream);
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
