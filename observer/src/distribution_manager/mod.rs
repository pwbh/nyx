use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex, MutexGuard},
};

mod broker;
mod partition;
mod topic;

pub use broker::Broker;

use crate::config::Config;

use self::{partition::Partition, topic::Topic};

#[derive(Debug)]
pub struct DistributionManager {
    pub brokers: Arc<Mutex<Vec<Broker>>>,
    topics: Vec<Arc<Mutex<Topic>>>,
    config: Config,
}

impl DistributionManager {
    pub fn new(config: Config) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            brokers: Arc::new(Mutex::new(vec![])),
            topics: vec![],
            config,
        }))
    }

    // Will return the broker id that has been added to the Observer
    pub fn create_broker(&mut self, stream: TcpStream) -> Result<String, String> {
        let mut brokers_lock = self.brokers.lock().unwrap();
        let broker = Broker::from(stream)?;
        self.spawn_broker_reader(&broker)?;
        let broker_id = broker.id.clone();
        brokers_lock.push(broker);
        //  rebalance(&mut brokers_lock);
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

        let mut replication_count = 0;

        if let Some(topic) = topic {
            let mut topic_lock = topic.lock().unwrap();
            // We've got 1 partition, and N replications for each partition (where N brokers count)
            topic_lock.partition_count += 1;

            let partition = Partition::new(&topic, topic_lock.partition_count);

            let replica_factor = self
                .config
                .get_number("replica_factor")
                .ok_or("Replica factor is not defined in the config, action aborted.")?;

            // Need to add partition replicas
            replicate_partitions(&mut brokers_lock, *replica_factor, &partition);

            // begin leadership race among topic's partitions
        } else {
            return Err(format!("Topic `{}` doesn't exist.", topic_name));
        }

        Ok(replication_count)
    }

    fn spawn_broker_reader(&self, broker: &Broker) -> Result<(), String> {
        let watch_stream = broker.stream.try_clone().map_err(|e| e.to_string())?;

        let brokers = Arc::clone(&self.brokers);
        let broker_id = broker.id.clone();

        std::thread::spawn(move || {
            let mut reader = BufReader::new(watch_stream);
            let mut buf = String::with_capacity(1024);

            loop {
                let size = reader.read_line(&mut buf).unwrap();
                if size == 0 {
                    println!("Broker {} has disconnected.", broker_id);
                    let mut brokers_lock = brokers.lock().unwrap();
                    if let Some(index) = brokers_lock.iter().position(|b| b.id == broker_id) {
                        brokers_lock.remove(index);
                    }
                    break;
                }
            }
        });
        Ok(())
    }
}

/*
TODO: When rebalancing, take into account the partition replica factor.
At the moment, rebalancing is not working in the intended way yet. Below is a cluster rebalancing with partition replica factor of 2.
replica factor should be configured in the config file.

   Cluster:
  Broker-1
  └─ Partition 0 of my_topic1 (Replica 1)
  └─ Partition 1 of my_topic1 (Replica 2)
  └─ Partition 0 of my_topic2 (Replica 1)
  └─ Partition 1 of my_topic2 (Replica 2)
  └─ Partition 0 of my_topic3 (Replica 1)
  └─ Partition 1 of my_topic3 (Replica 2)

  Broker-2
  └─ Partition 2 of my_topic1 (Replica 1)
  └─ Partition 3 of my_topic1 (Replica 2)
  └─ Partition 2 of my_topic2 (Replica 1)
  └─ Partition 3 of my_topic2 (Replica 2)
  └─ Partition 2 of my_topic3 (Replica 1)
  └─ Partition 3 of my_topic3 (Replica 2)

  Broker-3
  └─ Partition 0 of my_topic1 (Replica 2)
  └─ Partition 1 of my_topic1 (Replica 1)
  └─ Partition 0 of my_topic2 (Replica 2)
  └─ Partition 1 of my_topic2 (Replica 1)
  └─ Partition 0 of my_topic3 (Replica 2)
  └─ Partition 1 of my_topic3 (Replica 1)

*/
fn replicate_partitions(
    brokers_lock: &mut MutexGuard<'_, Vec<Broker>>,
    replica_factor: i32,
    partition: &Partition,
) {
    for replica_count in 1..=replica_factor {
        let least_distributed_broker = get_least_distributed_broker(brokers_lock);
        let replica = Partition::replicate(partition, replica_count as usize);
        least_distributed_broker.partitions.push(replica);
    }
}

fn get_least_distributed_broker<'a>(
    brokers_lock: &'a mut MutexGuard<'_, Vec<Broker>>,
) -> &'a mut Broker {
    let mut current_smallest = brokers_lock[0].partitions.len();
    let mut current_index = 0;
    let mut last_pushed_broker: Option<&Broker> = None;

    for (i, b) in brokers_lock.iter().enumerate() {
        // TODO: Make sure that if a replica has been added, that its not the same broker
        // as previous one that the replica was added to We dont want B1 -> P1-R1 and P1-R2
        if let Some(broker) = last_pushed_broker {
            if current_smallest > b.partitions.len() && broker.id != b.id {
                current_smallest = b.partitions.len();
                current_index = i;
                last_pushed_broker = Some(b);
            }
        } else {
            if current_smallest > b.partitions.len() {
                current_smallest = b.partitions.len();
                current_index = i;
                last_pushed_broker = Some(b);
            }
        }
    }

    return &mut brokers_lock[current_index];
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

        let distribution_manager = setup_distribution_for_tests(config, "5002");
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let topic_name = "notifications";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        let partition_replication_count_1 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_1,
            distribution_manager_lock.brokers.lock().unwrap().len()
        );

        let partition_replication_count_2 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_2,
            distribution_manager_lock.brokers.lock().unwrap().len()
        );

        let topic_name = "comments";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'comments'
        let partition_replication_count_3 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_3,
            distribution_manager_lock.brokers.lock().unwrap().len()
        );

        // Second partition for topic 'comments'
        let partition_replication_count_4 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_4,
            distribution_manager_lock.brokers.lock().unwrap().len()
        );

        let topic_name = "friend_requests";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'friend_requests'
        let partition_replication_count_5 = distribution_manager_lock
            .create_partition("friend_requests")
            .unwrap();

        assert_eq!(
            partition_replication_count_5,
            distribution_manager_lock.brokers.lock().unwrap().len()
        );

        println!("{:#?}", distribution_manager_lock.brokers);
    }
}
