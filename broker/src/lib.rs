use std::{
    net::TcpStream,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use partition::PartitionDetails;
use shared_structures::{Broadcast, DirManager, EntityType, Message, Metadata, Status, Topic};
use uuid::Uuid;

mod partition;

pub use partition::Partition;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LocalMetadata {
    id: String,
    pub partitions: Vec<Partition>,
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
            Ok(mut local_metadata) => {
                // Making sure to instantiatte a database for each local partition
                local_metadata.partitions = local_metadata
                    .partitions
                    .iter_mut()
                    .map(|p| Partition::from(p.details.clone(), custom_dir.as_ref()).unwrap())
                    .collect();

                Self {
                    stream,
                    local_metadata,
                    dir_manager,
                    cluster_metadata,
                    connected_producers,
                    addr,
                    custom_dir,
                }
            }
            Err(_e) => {
                let id = Uuid::new_v4().to_string();

                let local_metadata = LocalMetadata {
                    id,
                    partitions: vec![],
                };

                dir_manager.save(METADATA_FILE, &local_metadata)?;

                Self {
                    stream,
                    local_metadata,
                    dir_manager,
                    cluster_metadata,
                    connected_producers,
                    addr,
                    custom_dir,
                }
            }
        };

        broker.handshake()?;

        Ok(Arc::new(Mutex::new(broker)))
    }

    fn handshake(&mut self) -> Result<(), String> {
        Broadcast::to(
            &mut self.stream,
            &Message::EntityWantsToConnect {
                entity_type: EntityType::Broker,
            },
        )?;

        Broadcast::to(
            &mut self.stream,
            &Message::BrokerConnectionDetails {
                id: self.local_metadata.id.clone(),
                addr: self.addr.clone(),
            },
        )
    }

    pub fn handle_raw_message(
        &mut self,
        raw_data: &str,
        remote: Option<&mut TcpStream>,
    ) -> Result<(), String> {
        let message = serde_json::from_str::<Message>(raw_data).map_err(|e| e.to_string())?;
        self.handle_by_message(&message, remote)
    }

    // Messages from Producers and Observers are all processed here
    // maybe better to split it into two functions for clarity.
    fn handle_by_message(
        &mut self,
        message: &Message,
        remote: Option<&mut TcpStream>,
    ) -> Result<(), String> {
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
            Message::ClusterMetadata { metadata } => {
                println!("New metadata received from the cluster: {:#?}", metadata);
                self.cluster_metadata = metadata.clone();
                Ok(())
            }
            Message::RequestClusterMetadata => {
                if let Some(remote) = remote {
                    Broadcast::to(
                        remote,
                        &Message::ClusterMetadata {
                            metadata: self.cluster_metadata.clone(),
                        },
                    )
                } else {
                    Err(
                        "RequestClusterMetadata is missing the requesting remote stream"
                            .to_string(),
                    )
                }
            }
            Message::ProducerMessage {
                replica_id,
                payload,
            } => {
                println!("Received a message for partition replica {}!!!", replica_id);
                println!("Message: {:#?}", payload);

                if let Some(partition) = self
                    .local_metadata
                    .partitions
                    .iter_mut()
                    .find(|p| p.details.replica_id == *replica_id)
                {
                    partition.put(payload)
                } else {
                    Err("No corresponding partition replica was found on the broker.".to_string())
                }
            }
            _ => Err(format!(
                "Message {:?} is not handled in `handle_by_message`.",
                message
            )),
        }
    }

    fn handle_create_partition(
        &mut self,
        id: &str,
        replica_id: &str,
        topic: &Topic,
        replica_number: usize,
        partition_number: usize,
    ) -> Result<(), String> {
        let partition_details = PartitionDetails {
            id: id.to_string(),
            replica_id: replica_id.to_string(),
            status: Status::Up,
            topic: topic.clone(),
            role: shared_structures::Role::Follower,
            partition_number,
            replica_number,
        };
        let partition = Partition::from(partition_details, self.custom_dir.as_ref())?;
        self.local_metadata.partitions.push(partition);
        self.dir_manager.save(METADATA_FILE, &self.local_metadata)
    }
}

#[cfg(test)]
mod tests {}
