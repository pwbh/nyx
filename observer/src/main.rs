use clap::{arg, command};
use observer::{
    distribution_manager::{Broker, DistributionManager},
    Observer, DEV_CONFIG, PROD_CONFIG,
};
use shared_structures::{println_c, EntityType, Message, MessageDecoder, Reader, Role};
use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex, MutexGuard},
};

// TODO: Leader should delegate all messages to followers, for example it should delegate create broker commands to followers, etc.
fn main() -> Result<(), String> {
    let default_config_path_by_env = get_config_path_by_env();
    let matches = command!().arg(
        clap::Arg::new("config")
        .required(false)
    ).arg(
        arg!(-f --follow <HOST> "Runs the Observer as a follower for leader located at <HOST>, Host MUST by booted without -f flag.")
        .required(false)
    ).arg(
        arg!(-n --name <NAME> "Assigns a name to the broker, names are useful if you want to run two brokers on the same machine. Useful for nyx maintainers testing multi-node features.")
        .required(false)
    ).get_matches();

    let leader = matches.get_one::<String>("follow");
    let name = matches.get_one::<String>("name");

    let config_path = matches
        .get_one::<String>("config")
        .unwrap_or(&default_config_path_by_env);

    let role = if leader.is_none() {
        Role::Leader
    } else {
        Role::Follower
    };

    let mut observer = Observer::from(config_path, role, name)?;

    println_c(
        &format!(
            "Observer is ready to accept brokers on port {}",
            observer.listener.local_addr().unwrap().port()
        ),
        35,
    );

    let mut streams_distribution_manager = observer.distribution_manager.clone();

    // Connections listener
    std::thread::spawn(move || loop {
        let connection = observer.listener.incoming().next();

        if let Some(stream) = connection {
            match stream {
                Ok(stream) => {
                    if let Ok(message) = Reader::read_message(&stream) {
                        match message {
                            Message::FollowerWantsToConnect {
                                entity_type: EntityType::Observer,
                            } => {
                                match handle_connect_observer_follower(
                                    &mut streams_distribution_manager,
                                    stream,
                                ) {
                                    Ok(observer_follower_id) => {
                                        println!(
                                            "Observer follower connected {}",
                                            observer_follower_id
                                        )
                                    }
                                    Err(e) => {
                                        println!("Error while establishing connection: {}", e)
                                    }
                                }
                            }
                            Message::EntityWantsToConnect {
                                entity_type: EntityType::Broker,
                            } => match handle_connect_broker(
                                &mut streams_distribution_manager,
                                stream,
                            ) {
                                Ok(broker_id) => println!("Broker {} connected", broker_id),
                                Err(e) => {
                                    println!("Error while establishing connection: {}", e)
                                }
                            },
                            _ => {
                                println!("Handhsake failed, message could not be verified from connecting entity.")
                            }
                        }
                    } else {
                        println!("Could not decode the provided message, skipping connection.")
                    }
                }
                Err(e) => println!("Failed to establish basic TCP connection: {}", e),
            }
        }
    });

    let mut followers_distribution_manager = observer.distribution_manager.clone();

    // Leader obsrver exists, enabling the follower functionality
    if let Some(leader) = leader.cloned() {
        std::thread::spawn(move || {
            // TODO: connect to leader
            let leader_stream = TcpStream::connect(leader).unwrap();

            let mut reader = BufReader::new(&leader_stream);
            let mut buf = String::with_capacity(1024);

            loop {
                // TODO: constantly read delegated messages from leader
                let bytes_read = reader.read_line(&mut buf).unwrap();

                if bytes_read == 0 {
                    println!("Leader has closed connection. Exiting.");
                    break;
                }

                match handle_delegated_message(&buf, &mut followers_distribution_manager) {
                    Ok(_) => println!("Received delgated cluster metadata successfully"),
                    Err(e) => println!("Cluster metadata delegation error: {}", e),
                };
            }
        });
    }

    // This will make sure our main thread will never exit until the user will issue an EXIT command by himself
    loop {
        match observer.command_processor.process_raw_command() {
            Ok(command) => match command {
                observer::command_processor::Command {
                    name: observer::command_processor::CommandName::Create,
                    ..
                } => match handle_create_command(&mut observer.distribution_manager, &command) {
                    Ok(()) => println!("\x1b[38;5;2mOK\x1b[0m"),
                    Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
                },
                observer::command_processor::Command {
                    name: observer::command_processor::CommandName::List,
                    ..
                } => match handle_list_command(&mut observer.distribution_manager, &command) {
                    Ok(()) => println!("\x1b[38;5;2mOK\x1b[0m"),
                    Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
                },
            },
            Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
        };
    }
}

fn get_config_path_by_env() -> String {
    let file_name = if cfg!(debug_assertions) {
        DEV_CONFIG
    } else {
        PROD_CONFIG
    };

    format!("./config/{}", file_name)
}

fn handle_delegated_message(
    raw_message: &str,
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
) -> Result<(), String> {
    let delegated_message = MessageDecoder::decode(raw_message)?;

    match delegated_message {
        Message::ClusterMetadata { metadata } => {
            let mut distribution_manager_lock = distribution_manager.lock().unwrap();

            distribution_manager_lock.topics = metadata
                .topics
                .iter()
                .map(|t| Arc::new(Mutex::new(t.clone())))
                .collect();

            let mut brokers_lock = distribution_manager_lock.brokers.lock().unwrap();

            for broker in metadata.brokers {
                let broker = Broker::from(broker.id, None, broker.addr)?;
                brokers_lock.push(broker);
            }

            distribution_manager_lock.save_cluster_state()
        }
        _ => Err("Could not read delegated cluster metadata".to_string()),
    }
}

fn handle_list_command(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    command: &observer::command_processor::Command,
) -> Result<(), String> {
    let distribution_manager_lock = distribution_manager.lock().unwrap();
    let level = command.arguments.first().unwrap();

    if level == "ALL" {
        print_list_all(&distribution_manager_lock);
    } else {
        return Err(format!(
            "Requested listing depth `{}` is not supported",
            level
        ));
    }

    Ok(())
}

fn print_list_all(distribution_manager_lock: &MutexGuard<'_, DistributionManager>) {
    let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();

    println!(".");
    for broker in brokers_lock.iter() {
        println!("├── Broker {}", broker.id);
        for partition in broker.partitions.iter() {
            println!("│   ├── Partition {}", partition.id)
        }
    }
}

fn handle_create_command(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    command: &observer::command_processor::Command,
) -> Result<(), String> {
    let mut arguments_iter = command.arguments.iter();

    match arguments_iter.next() {
        Some(entity) => match entity.trim() {
            "TOPIC" => handle_create_topic(distribution_manager, &mut arguments_iter),
            "PARTITION" => handle_create_partition(distribution_manager, &mut arguments_iter),
            _ => Err("Unrecognized entity has been provided.".to_string()),
        },
        None => Err("Entity type was not provided.".to_string()),
    }
}

fn handle_connect_observer_follower(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    stream: TcpStream,
) -> Result<String, String> {
    let mut distribution_manager_lock = distribution_manager.lock().unwrap();
    let stream_addr = stream.peer_addr().map_err(|e| e.to_string())?;
    distribution_manager_lock.followers.push(stream);
    Ok(stream_addr.to_string())
}

fn handle_connect_broker(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    stream: TcpStream,
) -> Result<String, String> {
    let mut distribution_manager_lock = distribution_manager.lock().unwrap();
    distribution_manager_lock.connect_broker(stream)
}

fn handle_create_topic(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    arguments_iter: &mut std::slice::Iter<'_, String>,
) -> Result<(), String> {
    let topic_name = arguments_iter
        .next()
        .ok_or("Please provide topic name for which you want to create the topic.".to_string())?;
    let mut distribution_manager_lock: std::sync::MutexGuard<'_, DistributionManager> =
        distribution_manager.lock().unwrap();
    distribution_manager_lock.create_topic(topic_name)?;
    Ok(())
}

fn handle_create_partition(
    distribution_manager: &mut Arc<Mutex<DistributionManager>>,
    arguments_iter: &mut std::slice::Iter<'_, String>,
) -> Result<(), String> {
    let topic_name = arguments_iter.next().ok_or(
        "Please provide a valid topic name for which you want to create a partition.".to_string(),
    )?;
    let mut distribution_manager_lock = distribution_manager.lock().unwrap();
    distribution_manager_lock.create_partition(topic_name)?;
    Ok(())
}
