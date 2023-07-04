use std::{
    error::Error,
    io::{BufRead, BufReader},
    net::TcpStream,
    time::Duration,
};

use broker::Broker;

fn main() -> Result<(), Box<dyn Error>> {
    println_c("Initializing broker", 105);

    let addr = std::env::args().nth(1).unwrap();

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

    let stream = tcp_stream.ok_or("Stream is wrong")?;

    let mut broker = Broker::new(stream);

    broker.send_info()?;

    println!("{:#?}", broker);

    let exit = false;

    // Reader loop
    loop {
        if exit {
            break;
        }
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
