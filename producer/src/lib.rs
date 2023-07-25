use std::net::TcpStream;

use shared_structures::Broadcast;

pub struct Producer {
    mode: String,
    hosts: Vec<String>,
    streams: Vec<TcpStream>,
}

impl Producer {
    pub fn from(mode: &str, hosts: &[String]) -> Result<Self, String> {
        let mut producer = Self {
            mode: mode.to_string(),
            hosts: hosts.to_vec(),
            streams: vec![],
        };

        producer.connect()?;

        Ok(producer)
    }

    fn connect(&mut self) -> Result<(), String> {
        for host in self.hosts.iter() {
            let mut stream = TcpStream::connect(host).map_err(|e| e.to_string())?;
            Broadcast::to(
                &mut stream,
                &shared_structures::Message::ProducerWantsToConnect,
            )?;
            // TODO: should wait for the information from the broker that contains where do all the partitions for requested topic live in the cluster
            self.streams.push(stream);
        }

        Ok(())
    }
}
