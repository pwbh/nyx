use std::{io, str::SplitAsciiWhitespace};

mod utils;

fn main() {
    let hosts = parse_hosts();

    let mut buf = String::new();

    loop {
        io::stdin().read_line(&mut buf).unwrap();
        match handle_command(buf.trim()) {
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

fn handle_command(raw_command: &str) -> Result<String, String> {
    let mut tokens = raw_command.split_ascii_whitespace();
    let command = tokens.next().unwrap();

    match command {
        "CONNECT" => return handle_connect_command(&mut tokens),
        _ => return Err("unrecognized command has been passed.".to_string()),
    }
}

fn handle_connect_command(tokens: &mut SplitAsciiWhitespace<'_>) -> Result<String, String> {
    let hostname = match tokens.next() {
        Some(hostname) => hostname,
        None => return Err("hostname was not provided.".to_string()),
    };

    // do some logic for connecting

    Ok("OK".to_string())
}
