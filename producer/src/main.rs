use clap::{arg, command};
use producer::Producer;

fn main() -> Result<(), String> {
    let matches = command!()
        .arg(arg!(-b --brokers <BROKERS> "List of brokers to connect to").required(true))
        .arg(arg!(-t --topic <TOPIC> "The name of the topic onto which producer is going to push messages").required(true))
        .arg(arg!(-m --mode <MODE> "In which mode you want to run the producer 'test' or 'production', defaults to 'production'").required(false).default_value("production"))
        .get_matches();

    let mode = matches
        .get_one::<&str>("mode")
        .unwrap_or_else(|| &"production");

    let producer = Producer::from(mode, &[])?;

    Ok(())
}
