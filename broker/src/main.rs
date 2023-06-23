use std::{io, net::TcpStream, time::Duration};

fn main() {
    print_colored(
        r" ___ __  __ ____    ____  _____ ______     _______ ____  
|_ _|  \/  |  _ \  / ___|| ____|  _ \ \   / / ____|  _ \ 
 | || |\/| | |_) | \___ \|  _| | |_) \ \ / /|  _| | |_) |
 | || |  | |  __/   ___) | |___|  _ < \ V / | |___|  _ < 
|___|_|  |_|_|     |____/|_____|_| \_\ \_/  |_____|_| \_\
",
        105,
    );

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
}

fn print_colored(text: &str, color: usize) {
    let t = format!("\x1b[38;5;{}m{}\x1b[0m", color, text);
    println!("{}", t)
}
