mod utils;

fn main() {
    let hosts = parse_hosts();
    println!("{:?}", hosts);
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
