use std::{
    error::Error,
    io::{BufRead, BufReader},
    net::TcpStream,
    time::Duration,
};

use broker::{Broker, MessageHandler, Partition};
use clap::{arg, command};

fn main() -> Result<(), Box<dyn Error>> {
    let mut exit = false;

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

    let mut broker = Broker::new(stream, name)?;

    broker.handshake()?;

    println_c("Initialization complete.", 35);

    let mut reader = BufReader::new(&broker.stream);

    let mut buf = String::with_capacity(1024);

    // Reader loop
    loop {
        if exit {
            break;
        }

        let size = reader.read_line(&mut buf).map_err(|e| e.to_string())?;

        if size == 0 {
            println!("Connection with observer has been closed, exiting.");
            exit = true;
        }

        let result = MessageHandler::handle_incoming_message::<Partition>(&buf);

        println!("{:#?}", result);

        buf.clear();
    }

    Ok(())
}

fn println_c(text: &str, color: usize) {
    if color > 255 {
        panic!("Color is out of range 0 to 255");
    }

    let t = format!("\x1b[38;5;{}m{}\x1b[0m", color, text);
    println!("{}", t)
}
