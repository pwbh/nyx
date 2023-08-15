use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
};

use shared_structures::{Message, Metadata};

pub struct Producer {
    pub mode: String,
    brokers: Vec<String>,
    pub streams: Vec<TcpStream>,
    topic: String,
    pub cluster_metadata: Metadata,
}

impl Producer {
    pub fn from(brokers: &str, mode: &str, topic: &str) -> Result<Self, String> {
        let brokers: Vec<_> = brokers
            .split_terminator(',')
            .map(|b| b.to_string())
            .collect();

        if brokers.len() == 0 {
            return Err("No brokers were provided".to_string());
        }

        // Get metadata from the first broker we are connecting to (Doesn't really matter from which one)
        let stream = TcpStream::connect(&brokers[0]).map_err(|e| e.to_string())?;
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
                println!("{:#?}", cluster_metadata);

                let reader_stream = stream.try_clone().map_err(|e| e.to_string())?;

                let mut producer = Self {
                    mode: mode.to_string(),
                    brokers,
                    streams: vec![stream],
                    topic: topic.to_string(),
                    cluster_metadata,
                };

                // Open a reader connection to the borker we just read from the metadata, not to waste time on
                // closing and then re-opening the connection
                producer.open_broker_reader(reader_stream);

                producer.connect()?;

                Ok(producer)
            }
            _ => Err("Wrong message received".to_string()),
        }
    }

    fn connect(&mut self) -> Result<(), String> {
        for host in self.brokers.iter() {
            let mut stream = TcpStream::connect(host).map_err(|e| e.to_string())?;
            //    Broadcast::to(
            //        &mut stream,
            //        &shared_structures::Message::ProducerWantsToConnect {
            //            topic: self.topic.clone(),
            //        },
            //    )?;
            // TODO: should wait for the information from the broker that contains where do all the partitions for requested topic live in the cluster
            let stream_reader = stream.try_clone().map_err(|e| e.to_string())?;
            self.open_broker_reader(stream_reader);
            self.streams.push(stream);
        }

        Ok(())
    }

    fn open_broker_reader(&self, reader_stream: TcpStream) {
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
    }
}
