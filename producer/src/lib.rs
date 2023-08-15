use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
};

use shared_structures::{metadata::BrokerDetails, Broadcast, Message};

pub struct Producer {
    pub mode: String,
    pub broker_details: BrokerDetails,
    pub stream: TcpStream,
    pub topic: String,
}

impl Producer {
    pub fn from(brokers: &str, mode: &str, topic: &str) -> Result<Self, String> {
        let brokers: Vec<_> = brokers
            .split_terminator(',')
            .map(|b| b.to_string())
            .collect();

        if brokers.is_empty() {
            return Err("No brokers were provided".to_string());
        }

        // Get metadata from the first broker we are connecting to (Doesn't really matter from which one)
        // We are just looking for the broker that holds the leader to the topic we want to push to
        let mut stream = TcpStream::connect(&brokers[0]).map_err(|e| e.to_string())?;

        // Request cluster metadata from the first random broker we are conected to in the provided list
        Broadcast::to(
            &mut stream,
            &shared_structures::Message::RequestClusterMetadata,
        )?;

        let mut reader = BufReader::new(&stream);
        let mut buf = String::with_capacity(1024);

        let bytes_read = reader
            .read_line(&mut buf)
            .map_err(|e| format!("Producer: {}", e))?;

        if bytes_read == 0 {
            return Err("Got nothing from broker, something went wrong".to_string());
        }

        let message =
            serde_json::from_str::<Message>(&buf).map_err(|e| format!("Producer: {}", e))?;

        match message {
            shared_structures::Message::ClusterMetadata {
                metadata: cluster_metadata,
            } => {
                let broker = cluster_metadata
                    .brokers
                    .iter()
                    .find(|b| b.partitions.iter().any(|p| p.topic.name == topic));

                if let Some(broker_details) = broker {
                    // If the random broker we connected to happen to be the correct one,
                    // no need to reconnect already connected.
                    let stream = if stream.peer_addr().unwrap().to_string() == broker_details.addr {
                        stream
                    } else {
                        TcpStream::connect(&broker_details.addr).map_err(|e| e.to_string())?
                    };

                    println!("Opened connection to the relevant stream");

                    let producer = Self {
                        mode: mode.to_string(),
                        broker_details: broker_details.clone(),
                        stream,
                        topic: topic.to_string(),
                    };

                    producer.open_broker_reader()?;

                    Ok(producer)
                } else {
                    Err("Couldn't find such topic in the cluster, exiting.".to_string())
                }
            }
            _ => Err("Wrong message received".to_string()),
        }
    }

    fn open_broker_reader(&self) -> Result<(), String> {
        let reader_stream = self
            .stream
            .try_clone()
            .map_err(|e| format!("Producer: {}", e))?;

        std::thread::spawn(|| {
            let mut buf = String::with_capacity(1024);
            let mut reader = BufReader::new(reader_stream);

            loop {
                let bytes_read = reader.read_line(&mut buf).unwrap();

                if bytes_read == 0 {
                    break;
                }

                println!("Recieved message from broker: {:#?}", buf);
            }
        });

        Ok(())
    }
}
