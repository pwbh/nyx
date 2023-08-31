use std::{io::stdin, time::Instant};

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

    println!("Broadcasting messages...");

    let total_itteration = 50_000;

    let mut message_count = 0;

    let start_time = Instant::now();

    // ---------------------- Just for a test -------------------------
    loop {
        if message_count == total_itteration {
            break;
        }

        Broadcast::to(
            &mut producer.stream,
            &shared_structures::Message::ProducerMessage {
                replica_id: producer.destination_replica_id.clone(),
                payload: json!({
                  "_meta": {
                    "template_version": 0
                  },
                  "fixtures": [
                    {
                      "name": "cus_jenny_rosen",
                      "path": "/v1/customers",
                      "method": "post",
                      "params": {
                        "name": "Jenny Rosen",
                        "email": "jenny@rosen.com",
                        "source": "tok_visa",
                        "address": {
                          "line1": "1 Main Street",
                          "city": "New York"
                        }
                      }
                    },
                    {
                      "name": "ch_jenny_charge",
                      "path": "/v1/charges",
                      "method": "post",
                      "params": {
                        "customer": "${cus_jenny_rosen:id}",
                        "amount": 100,
                        "currency": "usd",
                        "capture": false
                      }
                    },
                    {
                      "name": "capt_bender",
                      "path": "/v1/charges/${ch_jenny_charge:id}/capture",
                      "method": "post"
                    }
                  ]
                }),
            },
        )?;
        message_count += 1;
    }
    // ----------------------------------------------------------------

    let end_time = Instant::now();
    let elapsed_time = end_time - start_time;

    let executions_per_second = (total_itteration as f64) / (elapsed_time.as_secs_f64());

    println!("Total message: {:?}", total_itteration);
    println!("Total time: {:?}", elapsed_time);
    println!("Executions per second: {:.2}", executions_per_second);

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
