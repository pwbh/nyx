use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

mod broker;
mod partition;
mod topic;

pub use broker::Broker;
pub use partition::Partition;
use shared_structures::Status;

use crate::{broadcast::Broadcast, config::Config};

use self::topic::Topic;

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

    // Will return the broker id that has been added to the Observer
    pub fn create_broker(&mut self, stream: TcpStream) -> Result<String, String> {
        let mut brokers_lock = self.brokers.lock().unwrap();
        let broker = Broker::from(stream)?;
        self.spawn_broker_reader(&broker)?;
        let broker_id = broker.id.clone();
        brokers_lock.push(broker);
        // Need to replicate the pending partitions if there is any
        replicate_pending_partitions(&mut self.pending_replication_partitions, &mut brokers_lock)?;
        Ok(broker_id)
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

        let topic = Topic::new(topic_name.to_string());
        self.topics.push(topic);

        Ok(topic_name.to_string())
    }

    // Need to rebalance if new partition is added to the broker
    pub fn create_partition(&mut self, topic_name: &str) -> Result<usize, String> {
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

            let partition = Partition::new(&topic, topic_lock.partition_count);

            drop(topic_lock);

            // Need to add partition replicas
            replicate_partition(
                &mut self.pending_replication_partitions,
                &mut brokers_lock,
                *replica_factor as usize,
                &partition,
            )?;

            // TODO: Should begin leadership race among replications of the Partition.
        } else {
            return Err(format!("Topic `{}` doesn't exist.", topic_name));
        }

        Ok(*replica_factor as usize)
    }

    // TODO: What should the distribution_manager do when there is only one broker, and it has disconnected due to a crash?
    // Distribution manager should start a failover, meaning it should find the replica that has disconnected and set all partition to PendingCreation
    // Each Partition should have a replica_id which is unique per replica to find it if such case occures.

    // TODO: What happens when a broker has lost connection? We need to find new leader.
    fn spawn_broker_reader(&self, broker: &Broker) -> Result<(), String> {
        let watch_stream = broker.stream.try_clone().map_err(|e| e.to_string())?;

        let brokers = Arc::clone(&self.brokers);
        let broker_id = broker.id.clone();

        let throttle = self
            .config
            .get_number("throttle")
            .ok_or("Throttle is missing form the configuration file.")?
            .clone();

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

                if size == 0 {
                    println!("Broker {} has disconnected.", broker_id);

                    let mut brokers_lock = brokers.lock().unwrap();

                    if let Some(broker) = brokers_lock.iter_mut().find(|b| b.id == broker_id) {
                        broker
                            .partitions
                            .iter_mut()
                            .for_each(|p| p.status = Status::Down);

                        let offline_partitions: Vec<_> = broker
                            .partitions
                            .iter()
                            .map(|p| (&p.id, &p.replica_id, p.replica_count))
                            .collect();

                        println!("Partion ID\t\t\t\tReplica ID\t\t\t\tReplica Count");
                        for offline_partition in offline_partitions.iter() {
                            println!(
                                "{}\t{}\t{}",
                                offline_partition.0, offline_partition.1, offline_partition.2
                            );
                        }
                    } else {
                        println!("Failed to find the Broker in the system, this can lead to major data loses.");
                        println!("Please let us know about this message by creating an issue on our GitHub repository https://github.com/pwbh/nyx/issues/new");
                    }

                    //   if let Some(index) = brokers_lock.iter().position(|b| b.id == broker_id) {
                    //       // TODO: Should I really remove the broker if it was disconnect? What if it crashed but all the data is still there?
                    //
                    //       brokers_lock.remove(index);
                    //   }
                    break;
                }
            }
        });
        Ok(())
    }
}

fn replicate_pending_partitions(
    pending_replication_partitions: &[(usize, Partition)],
    brokers_lock: &mut MutexGuard<'_, Vec<Broker>>,
) -> Result<(), String> {
    for (replications_needed, partition) in pending_replication_partitions.iter() {
        let last_replica_count = partition.replica_count;

        for replica_count in 1..=*replications_needed {
            let least_distributed_broker = get_least_distributed_broker(brokers_lock, partition);
            if let Some(broker) = least_distributed_broker {
                let replica = Partition::replicate(partition, last_replica_count + replica_count);
                broker.partitions.push(replica);
                Broadcast::create_partition(broker, &partition.id)?;
            }
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
    let total_available_brokers = brokers_lock.len();
    let future_replications_required = replica_factor as i32 - total_available_brokers as i32;

    if future_replications_required > 0 {
        // total_available_brokers - is the next replication that should be added by the count.
        let replica = Partition::replicate(partition, total_available_brokers);
        pending_replication_partitions.push((future_replications_required as usize, replica));
    }

    for replica_count in 1..=replica_factor {
        let least_distributed_broker = get_least_distributed_broker(brokers_lock, partition);
        if let Some(broker) = least_distributed_broker {
            let replica = Partition::replicate(partition, replica_count);
            broker.partitions.push(replica);
            Broadcast::create_partition(broker, &partition.id)?;
        }
    }

    Ok(())
}

fn get_clean_broker_from_partition_index<'a>(
    brokers_lock: &'a mut MutexGuard<'_, Vec<Broker>>,
    partition: &'a Partition,
) -> Option<usize> {
    for (i, b) in brokers_lock.iter().enumerate() {
        if b.partitions.iter().all(|p| p.id != partition.id) {
            return Some(i);
        }
    }

    return None;
}

fn get_least_distributed_broker<'a>(
    brokers_lock: &'a mut MutexGuard<'_, Vec<Broker>>,
    partition: &'a Partition,
) -> Option<&'a mut Broker> {
    if let Some(clean_from_partition_broker_index) =
        get_clean_broker_from_partition_index(brokers_lock, partition)
    {
        return Some(&mut brokers_lock[clean_from_partition_broker_index]);
    }

    return None;
}

#[cfg(test)]
mod tests {
    use std::{io::Write, net::TcpListener};

    use super::*;

    fn config_mock() -> Config {
        Config::from("../config/dev.properties".into()).unwrap()
    }

    fn mock_connecting_broker(addr: &str) -> TcpStream {
        let mut mock_stream = TcpStream::connect(&addr).unwrap();
        let payload = format!("{}\n", uuid::Uuid::new_v4().to_string());
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

        return mock_stream;
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
            distribution_manager_lock.create_broker(stream).unwrap();
        }

        drop(distribution_manager_lock);

        return distribution_manager;
    }

    #[test]
    fn create_brokers_works_as_expected() {
        let config = config_mock();
        let distribution_manager: Arc<Mutex<DistributionManager>> =
            DistributionManager::new(config);
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let addr = "localhost:8989";
        let listener = TcpListener::bind(addr).unwrap();

        std::thread::spawn(move || loop {
            listener.accept().unwrap();
        });

        let mock_stream_1 = TcpStream::connect(addr).unwrap();

        let result = distribution_manager_lock.create_broker(mock_stream_1);

        assert!(result.is_ok())
    }

    #[test]
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

    #[test]
    fn craete_partition_distributes_replicas() {
        let config = config_mock();

        let replica_factor = config.get_number("replica_factor").unwrap().clone();

        let distribution_manager = setup_distribution_for_tests(config, "5002");
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let topic_name = "notifications";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        let partition_replication_count_1 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_1, replica_factor as usize);

        let partition_replication_count_2 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_2, replica_factor as usize);

        let topic_name = "comments";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'comments'
        let partition_replication_count_3 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_3, replica_factor as usize);

        // Second partition for topic 'comments'
        let partition_replication_count_4 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_4, replica_factor as usize);

        let topic_name = "friend_requests";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'friend_requests'
        let partition_replication_count_5 = distribution_manager_lock
            .create_partition("friend_requests")
            .unwrap();

        assert_eq!(partition_replication_count_5, replica_factor as usize);

        let total_replicas = partition_replication_count_1
            + partition_replication_count_2
            + partition_replication_count_3
            + partition_replication_count_4
            + partition_replication_count_5;

        let total_replicas_in_brokers: usize = distribution_manager_lock
            .brokers
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.partitions.len())
            .sum();

        assert_eq!(total_replicas, total_replicas_in_brokers);

        println!("{:#?}", distribution_manager_lock.brokers);
    }
}
