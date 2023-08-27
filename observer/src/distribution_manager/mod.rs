use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

mod broker;
mod partition;

pub use broker::Broker;
pub use partition::Partition;
use shared_structures::{
    metadata::{BrokerDetails, PartitionDetails},
    Broadcast, Message, Metadata, Status, Topic,
};

use crate::config::Config;

#[derive(Debug)]
pub struct DistributionManager {
    pub brokers: Arc<Mutex<Vec<Broker>>>,
    topics: Vec<Arc<Mutex<Topic>>>,
    config: Config,
    pending_replication_partitions: Vec<(usize, Partition)>,
}

impl DistributionManager {
    pub fn new(config: Config) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            brokers: Arc::new(Mutex::new(vec![])),
            topics: vec![],
            config,
            pending_replication_partitions: vec![],
        }))
    }

    // Will return the broker id that has been added or restored to the Observer
    // TODO: Should check whether a broker that is being added already exists in the system + if it's Status is `Down`
    // meaning that this broker has disconnected in one of many possible ways, including user interference, unexpected system crush
    // or any other reason. Observer should try and sync with the brokers via the brokers provided id.
    pub fn connect_broker(&mut self, stream: TcpStream) -> Result<String, String> {
        // Handshake process between the Broker and Observer happening in get_broker_metadata
        let (id, addr, stream) = self.get_broker_metadata(stream)?;
        let mut brokers_lock = self.brokers.lock().unwrap();
        let broker_id =
            if let Some(disconnected_broker) = brokers_lock.iter_mut().find(|b| b.id == id) {
                disconnected_broker.restore(stream, addr)?;
                self.spawn_broker_reader(disconnected_broker)?;
                disconnected_broker.id.clone()
            } else {
                let mut broker = Broker::from(id, Some(stream), addr)?;
                self.spawn_broker_reader(&broker)?;
                let broker_id = broker.id.clone();
                // Need to replicate the pending partitions if there is any
                replicate_pending_partitions_once(
                    &mut self.pending_replication_partitions,
                    &mut broker,
                )?;
                brokers_lock.push(broker);
                broker_id
            };

        self.send_cluster_metadata(&mut brokers_lock)?;

        Ok(broker_id)
    }

    fn send_cluster_metadata(&self, brokers: &mut [Broker]) -> Result<(), String> {
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
                    })
                    .collect(),
            })
            .collect();

        let mut streams: Vec<_> = brokers
            .iter_mut()
            .filter(|b| b.stream.is_some())
            .map(|b| b.stream.as_mut().unwrap())
            .collect();

        let topics: Vec<_> = self
            .topics
            .iter()
            .map(|t| {
                let t_lock = t.lock().unwrap();
                t_lock.clone()
            })
            .collect();

        Broadcast::all(
            &mut streams[..],
            &shared_structures::Message::ClusterMetadata {
                metadata: Metadata {
                    brokers: metadata_brokers,
                    topics,
                },
            },
        )?;

        Ok(())
    }

    // Will return the name of created topic on success
    pub fn create_topic(&mut self, topic_name: &str) -> Result<String, String> {
        let brokers_lock = self.brokers.lock().unwrap();

        if brokers_lock.len() == 0 {
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
    pub fn create_partition(&mut self, topic_name: &str) -> Result<String, String> {
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
            )?;

            self.send_cluster_metadata(&mut brokers_lock)?;

            Ok(partition.id.clone())

            // TODO: Should begin leadership race among replications of the Partition.
        } else {
            Err(format!("Topic `{}` doesn't exist.", topic_name))
        }
    }

    fn get_broker_metadata(
        &self,
        stream: TcpStream,
    ) -> Result<(String, String, TcpStream), String> {
        let mut buf = String::with_capacity(1024);

        let mut reader = BufReader::new(&stream);

        let bytes_read = reader.read_line(&mut buf).map_err(|e| e.to_string())?;

        if bytes_read == 0 {
            return Err("Client exited unexpectadly during handhsake.".to_string());
        }

        let message = match serde_json::from_str::<Message>(&buf) {
            Ok(m) => m,
            Err(_) => {
                return Err(
                    "Handshake with client failed, unrecognized payload received".to_string(),
                )
            }
        };

        if let Message::BrokerConnectionDetails { id, addr } = message {
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
            let watch_stream = broker_stream.try_clone().map_err(|e| e.to_string())?;

            let brokers = Arc::clone(&self.brokers);
            let broker_id = broker.id.clone();

            let throttle = self
                .config
                .get_number("throttle")
                .ok_or("Throttle is missing form the configuration file.")?;

            std::thread::spawn(move || {
                let mut reader = BufReader::new(watch_stream);
                let mut buf = String::with_capacity(1024);

                loop {
                    let size = match reader.read_line(&mut buf) {
                        Ok(s) => s,
                        Err(e) => {
                            println!("Error in broker read thread: {}", e);
                            println!("Retrying to read with throttling at {}ms", throttle);
                            std::thread::sleep(Duration::from_millis(throttle as u64));
                            continue;
                        }
                    };

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
                }
            });
            Ok(())
        } else {
            println!("Ignoring spawning broker reader as Observer follower");
            Ok(())
        }
    }
}

pub fn broadcast_replicate_partition(
    broker: &mut Broker,
    replica: &mut Partition,
) -> Result<(), String> {
    if let Some(broker_tream) = &mut broker.stream {
        Broadcast::to(
            broker_tream,
            &Message::CreatePartition {
                id: replica.id.clone(),
                replica_id: replica.replica_id.clone(),
                topic: replica.topic.lock().unwrap().clone(),
                partition_number: replica.partition_number,
                replica_count: replica.replica_count,
            },
        )?;
    } else {
        println!("Ignoring broadcasting message as Observer follower")
    }
    // After successful creation of the partition on the broker,
    // we can set its status on the observer to Active.
    replica.status = Status::Up;

    Ok(())
}

fn replicate_pending_partitions_once(
    pending_replication_partitions: &mut Vec<(usize, Partition)>,
    new_broker: &mut Broker,
) -> Result<(), String> {
    for (replications_needed, partition) in pending_replication_partitions.iter_mut().rev() {
        let mut replica = Partition::replicate(partition, partition.replica_count + 1);
        broadcast_replicate_partition(new_broker, &mut replica)?;
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

fn replicate_partition(
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
        broadcast_replicate_partition(least_distributed_broker, &mut replica)?;
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
    use std::{io::Write, net::TcpListener};

    use super::*;

    fn config_mock() -> Config {
        Config::from("../config/dev.properties".into()).unwrap()
    }

    fn mock_connecting_broker(addr: &str) -> TcpStream {
        let mut mock_stream = TcpStream::connect(addr).unwrap();

        let mut payload = serde_json::to_string(&Message::BrokerConnectionDetails {
            id: uuid::Uuid::new_v4().to_string(),
            addr: "localhost:123123".to_string(),
        })
        .unwrap();
        payload.push('\n');
        mock_stream.write(payload.as_bytes()).unwrap();

        let read_stream = mock_stream.try_clone().unwrap();

        std::thread::spawn(|| {
            let mut reader = BufReader::new(read_stream);
            let mut buf = String::with_capacity(1024);

            loop {
                let size = reader.read_line(&mut buf).unwrap();

                println!("{}", buf);

                if size == 0 {
                    break;
                }
            }
        });

        mock_stream
    }

    fn setup_distribution_for_tests(config: Config, port: &str) -> Arc<Mutex<DistributionManager>> {
        let distribution_manager: Arc<Mutex<DistributionManager>> =
            DistributionManager::new(config);
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let addr = format!("localhost:{}", port);
        let listener = TcpListener::bind(&addr).unwrap();

        // Create 3 brokers to test the balancing of created partitions
        mock_connecting_broker(&addr);
        mock_connecting_broker(&addr);
        mock_connecting_broker(&addr);

        // Simulate acceptence of brokers
        for _ in 0..3 {
            let stream = listener.incoming().next().unwrap().unwrap();
            distribution_manager_lock.connect_broker(stream).unwrap();
        }

        drop(distribution_manager_lock);

        distribution_manager
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn create_brokers_works_as_expected() {
        let config = config_mock();
        let distribution_manager: Arc<Mutex<DistributionManager>> =
            DistributionManager::new(config);
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let addr = "localhost:0";
        let listener = TcpListener::bind(addr).unwrap();

        let connection_addr = format!("localhost:{}", listener.local_addr().unwrap().port());

        let spawned_thread = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
        });

        mock_connecting_broker(&connection_addr);

        let stream = spawned_thread.join().unwrap();

        let result = distribution_manager_lock.connect_broker(stream);

        println!("{:?}", result);

        assert!(result.is_ok())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn create_topic_fails_when_no_brokers() {
        let config = config_mock();
        let distribution_manager: Arc<Mutex<DistributionManager>> =
            DistributionManager::new(config);
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let topic_name = "new_user_registered";

        // Before
        let result = distribution_manager_lock
            .create_topic(topic_name)
            .unwrap_err();

        assert!(result.contains("No brokers have been found"));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn create_topic_works_as_expected_when_brokers_exist() {
        let config = config_mock();

        // After brokers have connnected to the Observer
        let distribution_manager = setup_distribution_for_tests(config, "5001");
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let topic_name = "new_user_registered";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        assert_eq!(distribution_manager_lock.topics.len(), 1);

        // We cant add the same topic name twice - Should error
        let result = distribution_manager_lock
            .create_topic(topic_name)
            .unwrap_err();

        assert!(result.contains("already exist."));

        distribution_manager_lock
            .create_topic("notification_resent")
            .unwrap();

        assert_eq!(distribution_manager_lock.topics.len(), 2);
        assert_eq!(
            distribution_manager_lock
                .topics
                .last()
                .unwrap()
                .lock()
                .unwrap()
                .name,
            "notification_resent"
        );
    }

    fn get_brokers_with_replicas(
        brokers_lock: &MutexGuard<'_, Vec<Broker>>,
        partition_id: &str,
    ) -> usize {
        brokers_lock
            .iter()
            .filter(|b| b.partitions.iter().find(|p| p.id == partition_id).is_some())
            .count()
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn create_partition_distributes_replicas() {
        let config = config_mock();

        let replica_factor = config.get_number("replica_factor").unwrap();

        let distribution_manager = setup_distribution_for_tests(config, "5002");
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let notifications_topic = "notifications";

        // Create 'notifications' topic
        distribution_manager_lock
            .create_topic(notifications_topic)
            .unwrap();

        // First partition for topic 'notifications'
        let partition_id_1 = distribution_manager_lock
            .create_partition(notifications_topic)
            .unwrap();

        let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();
        let total_brokers_with_replicas = get_brokers_with_replicas(&brokers_lock, &partition_id_1);
        assert_eq!(total_brokers_with_replicas, replica_factor as usize);
        drop(brokers_lock);

        // Second partition for topic 'notifications'
        let partition_id_2 = distribution_manager_lock
            .create_partition(notifications_topic)
            .unwrap();

        let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();
        let total_brokers_with_replicas = get_brokers_with_replicas(&brokers_lock, &partition_id_2);
        assert_eq!(total_brokers_with_replicas, replica_factor as usize);
        drop(brokers_lock);

        let comments_topic = "comments";

        // Create 'comments' topic
        distribution_manager_lock
            .create_topic(comments_topic)
            .unwrap();

        // First partition for topic 'comments'
        let partition_id_3 = distribution_manager_lock
            .create_partition(comments_topic)
            .unwrap();

        let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();
        let total_brokers_with_replicas = get_brokers_with_replicas(&brokers_lock, &partition_id_3);
        assert_eq!(total_brokers_with_replicas, replica_factor as usize);
        drop(brokers_lock);

        // Second partition for topic 'comments'
        let partition_id_4 = distribution_manager_lock
            .create_partition(comments_topic)
            .unwrap();

        let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();
        let total_brokers_with_replicas = get_brokers_with_replicas(&brokers_lock, &partition_id_4);
        assert_eq!(total_brokers_with_replicas, replica_factor as usize);
        drop(brokers_lock);

        let friend_requests_topic = "friend_requests";

        // Create 'friend_requests' topic
        distribution_manager_lock
            .create_topic(friend_requests_topic)
            .unwrap();

        // First partition for topic 'friend_requests'
        let partition_id_5 = distribution_manager_lock
            .create_partition("friend_requests")
            .unwrap();

        let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();
        let total_brokers_with_replicas = get_brokers_with_replicas(&brokers_lock, &partition_id_5);
        assert_eq!(total_brokers_with_replicas, replica_factor as usize);
        drop(brokers_lock);

        let mut unique_partitions = distribution_manager_lock
            .brokers
            .lock()
            .unwrap()
            .iter()
            .flat_map(|b| b.partitions.clone())
            .collect::<Vec<_>>();

        unique_partitions.dedup_by_key(|p| p.id.clone());

        println!("PARTITIONS: {:#?}", unique_partitions);

        let total_replicas_in_brokers: usize = distribution_manager_lock
            .brokers
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.partitions.len())
            .sum();

        assert_eq!(unique_partitions.len(), total_replicas_in_brokers);

        // Testing that all brokers have the same amount of partition replicas (meaning it was balanced well - max fault tolerence)
        let brokers_lock = distribution_manager_lock.brokers.lock().unwrap();

        if let Some((first, other)) = brokers_lock.split_first() {
            for broker in other {
                assert_eq!(first.partitions.len(), broker.partitions.len());
            }
        }

        drop(brokers_lock);

        println!("{:#?}", distribution_manager_lock.brokers);
    }
}
