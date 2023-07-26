use std::{
    error::Error,
    io::{BufRead, BufReader},
    net::{TcpListener, TcpStream},
    time::Duration,
};

use broker::{Broker, MessageHandler};
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

    let mut tcp_stream: Option<TcpStream> = None;
    let mut sleep_interval = 1000;

    // Should start trying to connect to the observer in intervals until success
    loop {
        match TcpStream::connect(&addr) {
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

    let mut broker = Broker::new(stream, host.to_string(), name)?;

    println_c(
        &format!(
            "Broker is ready to accept producers on port {}",
            listener.local_addr().unwrap().port()
        ),
        50,
    );

    let connected_producers = broker.connected_producers.clone();

    // Producers listener
    std::thread::spawn(move || loop {
        let connection = listener.incoming().next();

        if let Some(stream) = connection {
            match stream {
                Ok(stream) => connected_producers.lock().unwrap().push(stream),
                Err(e) => println!("Error: {}", e),
            }
        }
    });

    println_c("Initialization complete.", 35);

    let reader_stream = broker.stream.try_clone().map_err(|e| e.to_string())?;

    let mut message_handler = MessageHandler::from(&mut broker)?;

    let mut reader: BufReader<TcpStream> = BufReader::new(reader_stream);

    let mut buf = String::with_capacity(1024);

    // Reader loop
    loop {
        let size = reader.read_line(&mut buf).map_err(|e| e.to_string())?;

        if size == 0 {
            println!("Connection with observer has been closed, exiting.");
            break;
        }

        message_handler.handle_raw_message(&buf)?;

        buf.clear();
    }

    Ok(())
}
