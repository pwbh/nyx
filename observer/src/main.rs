use std::io;

mod command_processor;
mod utils;

use command_processor::CommandProcessor;

fn main() {
    let command_processor = CommandProcessor::new();

    let hosts = parse_hosts();

    let mut buf = String::new();

    loop {
        io::stdin().read_line(&mut buf).unwrap();
        match command_processor.process_raw_command(&buf) {
            Ok(status) => println!("\x1b[32m✓ {}\x1b[0m", status),
            Err(e) => println!("\x1b[38;5;1mERROR:\x1b[0m {}", e),
        };
        buf.clear();
    }
}

fn parse_hosts() -> Vec<String> {
    let hosts_arg = std::env::args().nth(1).expect(
        "Please provide a list of servers urls e.g. `server:port server:port server:port...`",
    );

    hosts_arg
        .split_whitespace()
        .map(|s| s.to_string())
        .collect()
}
