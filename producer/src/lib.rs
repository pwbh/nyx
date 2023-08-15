use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
};

use shared_structures::Broadcast;

pub struct Producer {
    pub mode: String,
    brokers: Vec<String>,
    pub streams: Vec<TcpStream>,
    topic: String,
}

impl Producer {
    pub fn from(brokers: &str, mode: &str, topic: &str) -> Result<Self, String> {
        let brokers = brokers
            .split_terminator(',')
            .map(|b| b.to_string())
            .collect();

        let mut producer = Self {
            mode: mode.to_string(),
            brokers,
            streams: vec![],
            topic: topic.to_string(),
        };

        producer.connect()?;

        Ok(producer)
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

    fn open_broker_reader(&self, stream_reader: TcpStream) {
        std::thread::spawn(|| {
            let mut buf = String::with_capacity(1024);
            let mut reader = BufReader::new(stream_reader);

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
