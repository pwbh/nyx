use clap::{arg, command};

fn main() {
    let matches = command!()
        .arg(arg!(-b --brokers <BROKERS> "List of brokers to connect to").required(true))
        .arg(arg!(-t --topic <TOPIC> "The name of the topic onto which producer is going to push messages").required(true))
        .get_matches();
}
