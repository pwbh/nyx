use clap::{arg, command};
use std::{
    collections::HashMap,
    fs,
    net::TcpListener,
    path::PathBuf,
    sync::{Arc, Mutex},
};

mod command_processor;
mod distribution_manager;
mod utils;

use command_processor::CommandProcessor;

use crate::distribution_manager::DistributionManager;

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
        None => println!("Booting as a leader"),
    };

    let config_path = matches.get_one::<String>("config").unwrap();
    let config = match load_config(config_path.into()) {
        Ok(c) => c,
        Err(e) => return Err(e),
    };

    println!("{:?}", config);

    let listener = match TcpListener::bind("localhost:3000") {
        Ok(l) => l,
        Err(e) => return Err(e.to_string()),
    };

    // Open a TCP stream for brokers to connect to

    println!("Observer is ready to accept brokers on port 3000");

    let mut distribution_manager = DistributionManager::new();
    let mut command_processor = CommandProcessor::new();

    let streams_distribution_manager = distribution_manager.clone();

    std::thread::spawn(move || loop {
        let stream = listener.incoming().next();

        if let Some(stream) = stream {
            match stream {
                Ok(stream) => {
                    let mut distribution_manager_lock =
                        streams_distribution_manager.lock().unwrap();

                    match distribution_manager_lock.create_broker(stream) {
                        Ok(broker) => {
                            println!(
                                "New broker connected {}",
                                broker.stream.peer_addr().unwrap()
                            )
                        }
                        Err(e) => println!("Broker read/write error: {}", e),
                    }
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
                    name: command_processor::CommandName::Create,
                    ..
                } => match handle_create_command(&mut distribution_manager, &command) {
                    Ok(()) => println!("\x1b[38;5;2mOK\x1b[0m"),
                    Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
                },
            },
            Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
        };
    }
}

fn handle_create_command(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    command: &command_processor::Command,
) -> Result<(), String> {
    match command.arguments.iter().next() {
        Some(entity) => match entity.as_str() {
            "TOPIC" => handle_create_topic(distribution_manager, &command.arguments[0]),
            "PARTITION" => handle_create_partition(distribution_manager, &command.arguments[0]),
            _ => Err("Unrecognized entity has been provided.".to_string()),
        },
        None => Err("Entity type was not provided.".to_string()),
    }
}

fn handle_create_topic(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    topic_name: &str,
) -> Result<(), String> {
    let mut distribution_manager_lock: std::sync::MutexGuard<'_, DistributionManager> =
        distribution_manager.lock().unwrap();
    distribution_manager_lock.create_topic(topic_name)?;
    Ok(())
}

fn handle_create_partition(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    topic_name: &str,
) -> Result<(), String> {
    let mut distribution_manager_lock = distribution_manager.lock().unwrap();
    distribution_manager_lock.create_partition(topic_name)?;
    Ok(())
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
        if line.chars().next().unwrap() == '#' {
            continue;
        }

        let split: Vec<&str> = line.split_terminator("=").collect();

        if split.len() != 2 {
            return Err("property format is incorrect, should be key=value".to_string());
        }

        let key = split[0].to_string();
        let value = if let Ok(f) = split[1].parse::<f32>() {
            if split[1].contains(".") {
                Value::Float(f)
            } else {
                Value::Number(split[1].parse::<i32>().unwrap())
            }
        } else if let Ok(i) = split[1].parse::<i32>() {
            Value::Number(i)
        } else {
            Value::String(split[1].to_string())
        };

        config.entry(key).or_insert(value);
    }

    return Ok(config);
}
