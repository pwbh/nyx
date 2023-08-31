use std::{
    error::Error,
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::Duration,
};

use broker::Broker;
use clap::{arg, command};
use shared_structures::println_c;

fn main() -> Result<(), Box<dyn Error>> {
    let matches = command!()
    .arg(clap::Arg::new("host")
        .required(true)
    )
    .arg(
        arg!(-n --name <NAME> "Assigns a name to the broker, names are useful if you want to run two brokers on the same machine. Useful for nyx maintainers testing multi-node features.")
        .required(false)
    ).get_matches();

    let addr = matches.get_one::<String>("host").unwrap();
    let name = matches.get_one::<String>("name");

    let log_name = match name {
        Some(n) => n,
        None => "broker",
    };

    println_c(&format!("Initializing {}", log_name), 105);

    let tcp_stream: Option<TcpStream>;
    let mut sleep_interval = 1000;

    // Should  trying to connect to the observer in intervals until success
    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                tcp_stream = Some(stream);
                println!("Connection with the Observer has been established");
                break;
            }

            Err(_) => {
                println!(
                    "Failed to connect to the Observer, next retry in {}s",
                    Duration::from_millis(sleep_interval).as_secs_f32()
                );
                std::thread::sleep(Duration::from_millis(sleep_interval));
                sleep_interval += 1500;
            }
        }
    }

    let stream: TcpStream = tcp_stream.ok_or("Stream is wrong")?;

    // Port 0 means that we let the system find a free port in itself and use that
    let listener = TcpListener::bind("localhost:0").map_err(|e| e.to_string())?;

    let host = listener.local_addr().unwrap();

    let broker = Broker::new(stream, host.to_string(), name)?;

    let broker_lock = broker.lock().unwrap();

    let reader_stream = broker_lock.stream.try_clone().map_err(|e| e.to_string())?;

    println_c(
        &format!(
            "Broker is ready to accept producers on port {}",
            listener.local_addr().unwrap().port()
        ),
        50,
    );

    let connected_producers = broker_lock.connected_producers.clone();

    let broker_for_producers = broker.clone();

    // Producers listener
    std::thread::spawn(move || loop {
        let connection = listener.incoming().next();

        if let Some(stream) = connection {
            match stream {
                Ok(stream) => {
                    if let Ok(read_stream) = stream.try_clone() {
                        connected_producers.lock().unwrap().push(read_stream);
                        match handshake_with_producer(stream, broker_for_producers.clone()) {
                            Ok(()) => {}
                            Err(e) => {
                                println!(
                                    "Error while handshaking with connectiong producer: {}",
                                    e
                                );
                            }
                        };
                    }
                }
                Err(e) => println!("Error: {}", e),
            }
        }
    });

    println!("Initial data on the broker:");

    for partition in broker_lock.local_metadata.partitions.iter() {
        if let Some(db) = &partition.database {
            println!("Partition {}: {}", partition.details.replica_id, db);
        }
    }

    drop(broker_lock);

    println_c("Initialization complete.", 35);

    let mut reader: BufReader<TcpStream> = BufReader::new(reader_stream);

    let mut buf = String::with_capacity(1024);

    // Reader loop
    loop {
        let size = reader.read_line(&mut buf).map_err(|e| e.to_string())?;

        if size == 0 {
            println!("Connection with observer has been closed, exiting.");
            break;
        }
        let mut broker_lock = broker.lock().unwrap();

        broker_lock.handle_raw_message(&buf, None)?;

        buf.clear();
    }

    Ok(())
}

fn handshake_with_producer(
    mut stream: TcpStream,
    broker: Arc<Mutex<Broker>>,
) -> Result<(), String> {
    let reader_stream = stream.try_clone().map_err(|e| {
        format!(
            "Broker: failed to instantiate a producer reader stream, ({})",
            e
        )
    })?;

    println!("Spawning reader: {:#?}", reader_stream);

    std::thread::spawn(move || {
        let mut buf = String::with_capacity(1024);
        let mut reader = BufReader::new(reader_stream);

        loop {
            let bytes_read = match reader.read_line(&mut buf) {
                Ok(b) => b,
                Err(e) => {
                    println!("Producer Read Stream Error: {}", e);
                    break;
                }
            };

            if bytes_read == 0 {
                println!("Producer is disconnect");
                break;
            }

            let mut broker_lock = broker.lock().unwrap();

            match broker_lock.handle_raw_message(&buf, Some(&mut stream)) {
                Ok(_) => {}
                Err(e) => {
                    println!("Failed to handle raw message: {}", e);
                    break;
                }
            };

            buf.clear();
        }
    });

    Ok(())
}
