use std::{
    net::TcpStream,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use shared_structures::{Broadcast, DirManager, Message, Metadata, Status, Topic};
use uuid::Uuid;

mod partition;

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
    pub fn new(
        stream: TcpStream,
        addr: String,
        name: Option<&String>,
    ) -> Result<Arc<Mutex<Self>>, String> {
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

        Ok(Arc::new(Mutex::new(broker)))
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

    pub fn handle_raw_message(&mut self, raw_data: &str) -> Result<(), String> {
        let message = serde_json::from_str::<Message>(raw_data).map_err(|e| e.to_string())?;
        self.handle_by_message(&message)
    }

    fn handle_by_message(&mut self, message: &Message) -> Result<(), String> {
        match message {
            Message::CreatePartition {
                id,
                replica_id,
                topic,
                replica_count,
                partition_number,
            } => self.handle_create_partition(
                id,
                replica_id,
                topic,
                *replica_count,
                *partition_number,
            ),
            Message::ProducerWantsToConnect { topic } => {
                println!("Producer wants to connect to topic `{}`", topic);
                // TODO: Should send back data with the locations (hosts) that hold the partition for given topic.
                Ok(())
            }
            Message::ClusterMetadata { metadata } => {
                println!("New metadata received from the cluster: {:#?}", metadata);
                self.cluster_metadata = metadata.clone();
                Ok(())
            }
            _ => Err(format!(
                "Message {:?} is not handled in `handle_by_message`.",
                message
            )),
        }
    }

    fn handle_create_partition(
        &mut self,
        id: &String,
        replica_id: &String,
        topic: &Topic,
        replica_count: usize,
        partition_number: usize,
    ) -> Result<(), String> {
        let partition = Partition::from(
            id.clone(),
            replica_id.clone(),
            Status::Up,
            topic.clone(),
            shared_structures::Role::Follower,
            partition_number,
            replica_count,
            self.custom_dir.as_ref(),
        )?;
        self.local_metadata.partitions.push(partition);
        self.dir_manager.save(METADATA_FILE, &self.local_metadata)
    }
}

#[cfg(test)]
mod tests {}
