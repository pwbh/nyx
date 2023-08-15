use std::io::stdin;

use clap::{arg, command};
use producer::Producer;

fn main() -> Result<(), String> {
    let matches = command!()
        .arg(arg!(-b --brokers <BROKERS> "List of brokers to connect to seperated by comma e.g. localhost:3000,localhost:4000,...").required(true))
        .arg(arg!(-t --topic <TOPIC> "The name of the topic onto which producer is going to push messages").required(true))
        .arg(arg!(-m --mode <MODE> "In which mode you want to run the producer 'test' or 'production', defaults to 'production'").required(false).default_value("production"))
        .get_matches();

    let brokers = matches.get_one::<String>("brokers").unwrap();
    let mode = matches.get_one::<String>("mode").unwrap();
    let topic = matches.get_one::<String>("topic").unwrap();

    let producer = Producer::from(brokers, mode, topic)?;

    let mut buf = String::with_capacity(1024);

    loop {
        stdin().read_line(&mut buf).unwrap();

        if buf == "EXIT" {
            break;
        }

        buf.clear()
    }

    Ok(())
}
