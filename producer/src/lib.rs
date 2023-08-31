use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
};

use shared_structures::{metadata::BrokerDetails, Broadcast, Reader};

pub struct Producer {
    pub mode: String,
    pub broker_details: BrokerDetails,
    pub stream: TcpStream,
    pub topic: String,
    pub destination_replica_id: String,
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

        let message = Reader::read_one_message(&mut stream)?;

        match message {
            shared_structures::Message::ClusterMetadata {
                metadata: cluster_metadata,
            } => {
                let broker_details = cluster_metadata
                    .brokers
                    .iter()
                    .find(|b| b.partitions.iter().any(|p| p.topic.name == topic))
                    .ok_or("Broker with desired partition has not been found.")?;

                let partition_details = broker_details
                    .partitions
                    .iter()
                    .find(|p| p.topic.name == topic)
                    .ok_or("Couldn't find the desited partition on selected broker")?;

                // If the random broker we connected to happen to be the correct one,
                // no need to reconnect already connected.

                let peer_addr = stream.peer_addr().map_err(|e| format!("Producer: {}", e))?;

                let stream = if peer_addr.to_string() == broker_details.addr {
                    stream
                } else {
                    TcpStream::connect(&broker_details.addr).map_err(|e| e.to_string())?
                };

                println!("Stream: {:#?}", stream);

                let producer = Self {
                    mode: mode.to_string(),
                    broker_details: broker_details.clone(),
                    stream,
                    topic: topic.to_string(),
                    destination_replica_id: partition_details.replica_id.clone(),
                };

                producer.open_broker_reader()?;

                Ok(producer)
            }
            _ => Err("Wrong message received on handshake".to_string()),
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

                buf.clear();
            }
        });

        Ok(())
    }
}
