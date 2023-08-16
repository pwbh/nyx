use std::io::stdin;

use clap::{arg, command};
use producer::Producer;
use serde_json::json;
use shared_structures::Broadcast;

fn main() -> Result<(), String> {
    let matches = command!()
        .arg(arg!(-b --brokers <BROKERS> "List of brokers to connect to seperated by comma e.g. localhost:3000,localhost:4000,...").required(true))
        .arg(arg!(-t --topic <TOPIC> "The name of the topic onto which producer is going to push messages").required(true))
        .arg(arg!(-m --mode <MODE> "In which mode you want to run the producer 'test' or 'production', defaults to 'production'").required(false).default_value("production"))
        .get_matches();

    let brokers = matches.get_one::<String>("brokers").unwrap();
    let mode = matches.get_one::<String>("mode").unwrap();
    let topic = matches.get_one::<String>("topic").unwrap();

    let mut producer = Producer::from(brokers, mode, topic)?;

    println!("Broker details: {:#?}", producer.broker_details);

    println!("Broadcasting a test message to the partition");

    Broadcast::to(
        &mut producer.stream,
        &shared_structures::Message::ProducerMessage {
            replica_id: producer.destination_replica_id,
            payload: json!({"message": "test"}),
        },
    )?;

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
