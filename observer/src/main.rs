use clap::{arg, command};
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    thread::JoinHandle,
    time::Duration,
};

mod command_processor;
mod utils;

use command_processor::CommandProcessor;

const DEFAULT_CONFIG_PATH: &str = "./config/dev.properties";

fn main() -> Result<(), String> {
    let matches = command!().arg(
        arg!(-f --follow <HOST> "Runs the Observer as a follower for leader located at <HOST>, Host MUST by booted without -f flag.")
        .required(false)
    ).arg(arg!(-c --config <PATH> "Config file to use when starting Observer.")
        .required(false)
        .default_value(DEFAULT_CONFIG_PATH)
    ).get_matches();

    match matches.get_one::<String>("follow") {
        Some(host) => println!("Booting as a follower for {}", host),
        None => println!("Bootin as a leader"),
    };

    let config_path = matches.get_one::<String>("config").unwrap();
    let config = match load_config(config_path.into()) {
        Ok(c) => c,
        Err(e) => return Err(e),
    };

    println!("{:?}", config);

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
            Ok(command) => match command {
                command_processor::Command {
                    name: command_processor::CommandName::Connect,
                    ..
                } => handle_connect_command(&command),

                _ => println!("sd"),
            },
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

fn handle_connect_command(command: &command_processor::Command) {
    let hostname = match command.arguments.iter().next() {
        Some(hostname) => hostname,
        None => {
            println!("hostname was not provided.");
            return;
        }
    };

    // do some logic for connecting
}

#[derive(Debug)]
enum Value {
    String(String),
    Number(i32),
    Float(f32),
}

fn load_config(path: PathBuf) -> Result<HashMap<String, Value>, String> {
    let mut config = HashMap::new();

    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) => return Err(e.to_string()),
    };

    for line in content.lines() {
        let split: Vec<&str> = line.split_terminator("=").collect();

        if split.len() != 2 {
            return Err("property format is incorrect, should be key=value".to_string());
        }

        let key = split[0].to_string();
        let value = if let Ok(f) = split[1].parse::<f32>() {
            Value::Float(f)
        } else if let Ok(i) = split[1].parse::<i32>() {
            Value::Number(i)
        } else {
            Value::String(split[1].to_string())
        };

        config.entry(key).or_insert(value);
    }

    return Ok(config);
}
