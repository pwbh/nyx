use std::{
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

mod broker;
mod partition;

pub use broker::Broker;
pub use partition::Partition;
use shared_structures::{
    metadata::{BrokerDetails, PartitionDetails},
    Broadcast, DirManager, Message, Metadata, Reader, Status, Topic,
};
use tokio::io::AsyncBufReadExt;

use crate::{config::Config, CLUSTER_FILE};

#[derive(Debug)]
pub struct DistributionManager {
    pub brokers: Arc<Mutex<Vec<Broker>>>,
    pub topics: Vec<Arc<Mutex<Topic>>>,
    pub cluster_dir: DirManager,
    pub followers: Vec<tokio::net::TcpStream>,
    config: Config,
    pending_replication_partitions: Vec<(usize, Partition)>,
}

impl DistributionManager {
    pub fn from(config: Config, name: Option<&String>) -> Result<Arc<Mutex<Self>>, String> {
        let custom_dir = if let Some(name) = name {
            let custom_path = format!("/observer/{}/", name);
            Some(PathBuf::from(custom_path))
        } else {
            Some(PathBuf::from("/observer"))
        };

        println!("Custom dir: {:?}", custom_dir);

        let cluster_dir = DirManager::with_dir(custom_dir.as_ref());

        let cluster_metadata = match cluster_dir.open::<Metadata>(CLUSTER_FILE) {
            Ok(m) => m,
            Err(_) => Metadata::default(),
        };

        let mut distribution_manager = Self {
            brokers: Arc::new(Mutex::new(vec![])),
            topics: vec![],
            config,
            pending_replication_partitions: vec![],
            cluster_dir,
            followers: vec![],
        };

        distribution_manager.load_cluster_state(&cluster_metadata)?;

        Ok(Arc::new(Mutex::new(distribution_manager)))
    }

    pub fn load_cluster_state(&mut self, cluster_metadata: &Metadata) -> Result<(), String> {
        self.topics = cluster_metadata
            .topics
            .iter()
            .map(|t| Arc::new(Mutex::new(t.clone())))
            .collect();

        let mut brokers_lock = self.brokers.lock().unwrap();

        for b in cluster_metadata.brokers.iter() {
            let partitions: Vec<_> = b
                .partitions
                .iter()
                .map(|p| {
                    // Theoratically we should never have a case where we don't find the topic of the
                    // partition in the system, this is why I allow myself to unwrap here, and crash the system
                    // if such case occures (Indicates a serious bug in the system).
                    let topic = self
                        .topics
                        .iter()
                        .find(|t| {
                            let t_lock = t.lock().unwrap();

                            t_lock.name == p.topic.name
                        })
                        .unwrap();

                    Partition {
                        id: p.id.clone(),
                        replica_id: p.replica_id.clone(),
                        partition_number: p.partition_number,
                        replica_count: p.replica_count,
                        role: p.role,
                        status: Status::Down,
                        topic: topic.clone(),
                    }
                })
                .collect();

            let offline_broker = Broker {
                id: b.id.clone(),
                partitions,
                stream: None,
                status: Status::Down,
                addr: b.addr.clone(),
            };

            brokers_lock.push(offline_broker);
        }

        Ok(())
    }

    pub fn save_cluster_state(&self) -> Result<(), String> {
        let metadata = self.get_cluster_metadata()?;
        self.cluster_dir.save(CLUSTER_FILE, &metadata)
    }

    // Will return the broker id that has been added or restored to the Observer
    // TODO: Should check whether a broker that is being added already exists in the system + if it's Status is `Down`
    // meaning that this broker has disconnected in one of many possible ways, including user interference, unexpected system crush
    // or any other reason. Observer should try and sync with the brokers via the brokers provided id.
    pub async fn connect_broker(
        &mut self,
        stream: tokio::net::TcpStream,
    ) -> Result<String, String> {
        println!("NEW BROKER: {:?}", stream);
        // Handshake process between the Broker and Observer happening in get_broker_metadata
        let (id, addr, stream) = self.get_broker_metadata(stream).await?;
        println!("BROKER METADATA: {} {} {:?}", id, addr, stream);
        let mut brokers_lock = self.brokers.lock().unwrap();
        println!("AQUIRED BROKER LOCK");
        let broker_id =
            if let Some(disconnected_broker) = brokers_lock.iter_mut().find(|b| b.id == id) {
                disconnected_broker.restore(stream, addr)?;
                self.spawn_broker_reader(disconnected_broker)?;
                disconnected_broker.id.clone()
            } else {
                let mut broker = Broker::from(id, Some(stream), addr)?;
                self.spawn_broker_reader(&broker)?;
                // Need to replicate the pending partitions if there is any
                replicate_pending_partitions_once(
                    &mut self.pending_replication_partitions,
                    &mut broker,
                )
                .await?;
                let broker_id = broker.id.clone();
                brokers_lock.push(broker);
                broker_id
            };

        // Releaseing lock for broadcast_cluster_metadata
        drop(brokers_lock);

        self.broadcast_cluster_metadata().await?;

        Ok(broker_id)
    }

    pub fn get_cluster_metadata(&self) -> Result<Metadata, String> {
        let brokers = self.brokers.lock().unwrap();

        let metadata_brokers: Vec<BrokerDetails> = brokers
            .iter()
            .map(|b| BrokerDetails {
                id: b.id.clone(),
                addr: b.addr.clone(),
                status: b.status,
                partitions: b
                    .partitions
                    .iter()
                    .map(|p| PartitionDetails {
                        id: p.id.clone(),
                        replica_id: p.replica_id.to_string(),
                        role: p.role,
                        topic: p.topic.lock().unwrap().clone(),
                        partition_number: p.partition_number,
                        replica_count: p.replica_count,
                    })
                    .collect(),
            })
            .collect();

        let topics: Vec<_> = self
            .topics
            .iter()
            .map(|t| {
                let t_lock = t.lock().unwrap();
                t_lock.clone()
            })
            .collect();

        Ok(Metadata {
            brokers: metadata_brokers,
            topics,
        })
    }

    async fn broadcast_cluster_metadata(&mut self) -> Result<(), String> {
        let metadata = self.get_cluster_metadata()?;

        self.save_cluster_state()?;

        let mut brokers = self.brokers.lock().unwrap();

        let mut broker_streams: Vec<_> = brokers
            .iter_mut()
            .filter_map(|b| b.stream.map(|s| s.lock()).as_mut())
            .collect();

        let mut followers_streams: Vec<_> = self.followers.iter_mut().collect();

        println!("Followers: {:?}", followers_streams);

        let message = shared_structures::Message::ClusterMetadata { metadata };

        println!("BROADCASTING!!!");

        Broadcast::all_tokio(&mut followers_streams[..], &message).await?;
        Broadcast::all_tokio(&mut broker_streams[..], &message).await
    }

    // Will return the name of created topic on success
    pub fn create_topic(&mut self, topic_name: &str) -> Result<String, String> {
        let brokers_lock = self.brokers.lock().unwrap();

        let available_brokers = brokers_lock
            .iter()
            .filter(|b| b.status == Status::Up)
            .count();

        if available_brokers == 0 {
            return Err(
                "No brokers have been found, please make sure at least one broker is connected."
                    .to_string(),
            );
        }

        let topic_exists = self.topics.iter().any(|t| {
            let t = t.lock().unwrap();
            t.name == *topic_name
        });

        if topic_exists {
            return Err(format!("Topic `{}` already exist.", topic_name));
        }

        let topic = Topic::new_shared(topic_name.to_string());
        self.topics.push(topic);

        Ok(topic_name.to_string())
    }

    // Need to rebalance if new partition is added to the broker
    pub async fn create_partition(&mut self, topic_name: &str) -> Result<String, String> {
        let mut brokers_lock = self.brokers.lock().unwrap();

        if brokers_lock.len() == 0 {
            return Err(
                "No brokers have been found, please make sure at least one broker is connected."
                    .to_string(),
            );
        }

        let topic = self.topics.iter_mut().find(|t| {
            let t = t.lock().unwrap();
            t.name == *topic_name
        });

        let replica_factor = self
            .config
            .get_number("replica_factor")
            .ok_or("Replica factor is not defined in the config, action aborted.")?;

        if let Some(topic) = topic {
            let mut topic_lock = topic.lock().unwrap();
            // We've got 1 partition, and N replications for each partition (where N brokers count)
            topic_lock.partition_count += 1;

            let partition = Partition::new(topic, topic_lock.partition_count);

            drop(topic_lock);

            // Need to add partition replicas
            replicate_partition(
                &mut self.pending_replication_partitions,
                &mut brokers_lock,
                replica_factor as usize,
                &partition,
            )
            .await?;

            // Releaseing lock for broadcast_cluster_metadata
            drop(brokers_lock);

            self.broadcast_cluster_metadata().await?;

            Ok(partition.id.clone())

            // TODO: Should begin leadership race among replications of the Partition.
        } else {
            Err(format!("Topic `{}` doesn't exist.", topic_name))
        }
    }

    async fn get_broker_metadata(
        &self,
        mut stream: tokio::net::TcpStream,
    ) -> Result<(String, String, tokio::net::TcpStream), String> {
        if let Message::BrokerConnectionDetails { id, addr } =
            Reader::read_one_message_tokio(&mut stream).await?
        {
            Ok((id, addr, stream))
        } else {
            Err("Handshake with client failed, wrong message received from client.".to_string())
        }
    }

    // TODO: What should the distribution_manager do when there is only one broker, and it has disconnected due to a crash?
    // Distribution manager should start a failover, meaning it should find the replica that has disconnected and set all partition to PendingCreation
    // Each Partition should have a replica_id which is unique per replica to find it if such case occures.

    // TODO: What happens when a broker has lost connection? We need to find a new leader for all partition leaders.
    fn spawn_broker_reader(&self, broker: &Broker) -> Result<(), String> {
        if let Some(broker_stream) = &broker.stream {
            let brokers = Arc::clone(&self.brokers);
            let broker_id = broker.id.clone();

            let throttle = self
                .config
                .get_number("throttle")
                .ok_or("Throttle is missing form the configuration file.")?;

            let task_broker_stream = broker_stream.clone();

            tokio::spawn(async move {
                let mut buf = String::with_capacity(1024);

                loop {
                    let stream = task_broker_stream.lock().await;

                    let size = match stream.read_line(&mut buf).await {
                        Ok(s) => s,
                        Err(e) => {
                            println!("Error in broker read thread: {}", e);
                            println!("Retrying to read with throttling at {}ms", throttle);
                            std::thread::sleep(Duration::from_millis(throttle as u64));
                            continue;
                        }
                    };

                    drop(stream);

                    // TODO: Think what should happen to the metadata of the broker that has been disconnected.
                    if size == 0 {
                        println!("Broker {} has disconnected.", broker_id);

                        let mut brokers_lock = brokers.lock().unwrap();

                        if let Some(broker) = brokers_lock.iter_mut().find(|b| b.id == broker_id) {
                            broker.disconnect();

                            let offline_partitions: Vec<_> = broker
                                .get_offline_partitions()
                                .iter()
                                .map(|p| (&p.id, &p.replica_id, p.replica_count))
                                .collect();

                            for offline_partition in offline_partitions.iter() {
                                println!(
                                    "Broker {}:\t{}\t{}\t{}",
                                    broker.id,
                                    offline_partition.0,
                                    offline_partition.1,
                                    offline_partition.2
                                );
                            }
                        } else {
                            println!("Failed to find the Broker in the system, this can lead to major data loses.");
                            println!("Please let us know about this message by creating an issue on our GitHub repository https://github.com/pwbh/nyx/issues/new");
                        }
                        break;
                    }

                    buf.clear();
                }
            });

            Ok(())
        } else {
            println!("Ignoring spawning broker reader as Observer follower");
            Ok(())
        }
    }
}

pub async fn broadcast_replicate_partition(
    broker: &mut Broker,
    replica: &mut Partition,
) -> Result<(), String> {
    if let Some(broker_stream) = &mut broker.stream {
        let mut stream = broker_stream.lock().await;
        Broadcast::to_tokio(
            &mut stream,
            &Message::CreatePartition {
                id: replica.id.clone(),
                replica_id: replica.replica_id.clone(),
                topic: replica.topic.lock().unwrap().clone(),
                partition_number: replica.partition_number,
                replica_count: replica.replica_count,
            },
        )
        .await?;
    } else {
        println!("Ignoring broadcasting message as Observer follower")
    }
    // After successful creation of the partition on the broker,
    // we can set its status on the observer to Active.
    replica.status = Status::Up;

    Ok(())
}

async fn replicate_pending_partitions_once(
    pending_replication_partitions: &mut Vec<(usize, Partition)>,
    new_broker: &mut Broker,
) -> Result<(), String> {
    for (replications_needed, partition) in pending_replication_partitions.iter_mut().rev() {
        let mut replica = Partition::replicate(partition, partition.replica_count + 1);
        broadcast_replicate_partition(new_broker, &mut replica).await?;
        new_broker.partitions.push(replica);
        partition.replica_count += 1;
        *replications_needed -= 1;
    }

    println!("{:#?}", pending_replication_partitions);

    // Remove totally replicated partitions
    loop {
        let current = pending_replication_partitions.last();

        if let Some((pending_replications, _)) = current {
            if *pending_replications == 0 {
                pending_replication_partitions.pop();
            } else {
                break;
            }
        } else {
            break;
        }
    }

    Ok(())
}

async fn replicate_partition(
    pending_replication_partitions: &mut Vec<(usize, Partition)>,
    brokers_lock: &mut MutexGuard<'_, Vec<Broker>>,
    replica_factor: usize,
    partition: &Partition,
) -> Result<(), String> {
    // Here we create a variable containing the total available brokers in the cluster to check whether it is less
    // then replication factor, if so we certain either way that we will be able to replicate to all partitions
    let total_available_brokers = brokers_lock
        .iter()
        .filter(|b| b.status == Status::Up)
        .count();
    let mut future_replications_required = replica_factor as i32 - total_available_brokers as i32;

    if future_replications_required > 0 {
        // total_available_brokers - is the next replication that should be added by the count.
        let replica = Partition::replicate(partition, total_available_brokers);
        pending_replication_partitions.push((future_replications_required as usize, replica));
    } else {
        future_replications_required = 0;
    }

    let current_max_replications = replica_factor - future_replications_required as usize;

    for replica_count in 1..=current_max_replications {
        let least_distributed_broker = get_least_distributed_broker(brokers_lock, partition)?;
        let mut replica = Partition::replicate(partition, replica_count);
        broadcast_replicate_partition(least_distributed_broker, &mut replica).await?;
        least_distributed_broker.partitions.push(replica);
    }

    Ok(())
}

fn get_least_distributed_broker<'a>(
    brokers_lock: &'a mut MutexGuard<'_, Vec<Broker>>,
    partition: &'a Partition,
) -> Result<&'a mut Broker, String> {
    let mut brokers_lock_iter = brokers_lock.iter().enumerate();

    let first_element = brokers_lock_iter
        .next()
        .ok_or("At least 1 registerd broker is expected in the system.")?;

    let mut least_distribured_broker_index: usize = first_element.0;
    let mut least_distributed_broker: usize = first_element.1.partitions.len();

    for (i, b) in brokers_lock_iter {
        if b.partitions.iter().all(|p| p.id != partition.id)
            && least_distributed_broker > b.partitions.len()
        {
            least_distributed_broker = b.partitions.len();
            least_distribured_broker_index = i;
        }
    }

    Ok(&mut brokers_lock[least_distribured_broker_index])
}

#[cfg(test)]
mod tests {
    use super::*;
}
