use std::{
    net::TcpStream,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use shared_structures::{Broadcast, DirManager, Message, Metadata};
use uuid::Uuid;

mod message_handler;
mod partition;

pub use message_handler::MessageHandler;
pub use partition::Partition;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LocalMetadata {
    id: String,
    partitions: Vec<Partition>,
}

const METADATA_FILE: &str = "metadata.json";

#[derive(Debug)]
pub struct Broker {
    pub local_metadata: LocalMetadata,
    pub dir_manager: DirManager,
    pub cluster_metadata: Metadata,
    pub stream: TcpStream,
    pub connected_producers: Arc<Mutex<Vec<TcpStream>>>,
    pub addr: String,
    pub custom_dir: Option<PathBuf>,
}

impl Broker {
    /// Broker will automatically initiate a handshake with the Observer
    pub fn new(stream: TcpStream, addr: String, name: Option<&String>) -> Result<Self, String> {
        let custom_dir: Option<PathBuf> = name.map(|f| f.into());

        let cluster_metadata = Metadata { brokers: vec![] };

        let connected_producers = Arc::new(Mutex::new(vec![]));

        let dir_manager = DirManager::with_dir(custom_dir.as_ref());

        let mut broker = match dir_manager.open::<LocalMetadata>(METADATA_FILE) {
            Ok(local_metadata) => Self {
                stream,
                local_metadata,
                dir_manager,
                cluster_metadata,
                connected_producers,
                addr,
                custom_dir,
            },
            Err(_e) => {
                let id = Uuid::new_v4().to_string();

                let local_metadata = LocalMetadata {
                    id,
                    partitions: vec![],
                };

                dir_manager.save(METADATA_FILE, &local_metadata)?;

                let broker = Self {
                    stream,
                    local_metadata,
                    dir_manager,
                    cluster_metadata,
                    connected_producers,
                    addr,
                    custom_dir,
                };

                broker
            }
        };

        broker.handshake()?;

        Ok(broker)
    }

    fn handshake(&mut self) -> Result<(), String> {
        Broadcast::to(
            &mut self.stream,
            &Message::BrokerWantsToConnect {
                id: self.local_metadata.id.clone(),
                addr: self.addr.clone(),
            },
        )
    }

    fn create_partition(&mut self, partition: Partition) -> Result<(), String> {
        self.local_metadata.partitions.push(partition);
        self.dir_manager.save(METADATA_FILE, &self.local_metadata)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
