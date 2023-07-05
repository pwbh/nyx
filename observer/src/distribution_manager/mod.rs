use std::{
    io::{BufRead, BufReader},
    net::TcpStream,
    sync::{Arc, Mutex, MutexGuard},
};

mod broker;
mod partition;
mod topic;

pub use broker::Broker;

use self::{partition::Partition, topic::Topic};

#[derive(Debug)]
pub struct DistributionManager {
    pub brokers: Vec<Broker>,
    topics: Vec<Arc<Mutex<Topic>>>,
}

impl DistributionManager {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            brokers: Arc::new(Mutex::new(vec![])),
            topics: vec![],
        }))
    }

    // Need to rebalance if new broker is added
    pub fn create_broker(&mut self, stream: TcpStream) -> Result<String, String> {
        let mut brokers_lock = self.brokers.lock().unwrap();
        let broker = Broker::from(stream)?;
        brokers_lock.push(broker);
        let broker_index = brokers_lock.len() - 1;
        rebalance(&mut brokers_lock);
        let broker = brokers_lock.last().ok_or(
            "Failed to get newly created broker (last item doesnt exist, no brokers found?)."
                .to_string(),
        )?;
        self.spawn_broker_reader(&broker, broker_index)?;
        Ok(broker.id.clone())
    }

    pub fn create_topic(&mut self, topic_name: &str) -> Result<&Arc<Mutex<Topic>>, String> {
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

        self.topics
            .last()
            .ok_or("There is no topics on the Observer.".to_string())
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

            brokers_lock.iter_mut().for_each(|b| {
                // Replicate partition
                let replication = Partition::replicate(&partition, replication_count);
                b.partitions.push(replication);
                replication_count += 1;
            });

            // begin leadership race among topic's partitions
        } else {
            return Err(format!("Topic `{}` doesn't exist.", topic_name));
        }

        Ok(replication_count)
    }

    fn spawn_broker_reader(&self, broker: &Broker, broker_index: usize) -> Result<(), String> {
        let watch_stream = broker.stream.try_clone().map_err(|e| e.to_string())?;

        let brokers = Arc::clone(&self.brokers);

        std::thread::spawn(move || {
            let mut reader = BufReader::new(watch_stream);
            let mut buf = String::with_capacity(1024);

            loop {
                let size = reader.read_line(&mut buf).unwrap();
                if size == 0 {
                    let mut brokers_lock = brokers.lock().unwrap();
                    brokers_lock.remove(broker_index);
                    break;
                }
            }
        });
        Ok(())
    }
}

fn rebalance(brokers_lock: &mut MutexGuard<'_, Vec<Broker>>) {
    for i in 0..brokers_lock.len() {
        let (a, b) = brokers_lock.split_at_mut(i + 1);

        let current_broker = &mut a[i];

        for other_broker in b.iter_mut() {
            balance_brokers(current_broker, other_broker);
        }
    }
}

fn balance_brokers(current_broker: &mut Broker, other_broker: &mut Broker) {
    for current_partition in current_broker.partitions.iter() {
        let partition = other_broker
            .partitions
            .iter()
            .find(|p| current_partition.id == p.id);

        if let None = partition {
            let fresh_partition = Partition::from(current_partition);
            other_broker.partitions.push(fresh_partition);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    fn setup_distribution_for_tests() -> Arc<Mutex<DistributionManager>> {
        let distribution_manager = DistributionManager::new();
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let addr = "localhost:3000";
        let listener = TcpListener::bind(addr).unwrap();

        std::thread::spawn(move || loop {
            listener.accept().unwrap();
        });

        let mock_stream_1 = TcpStream::connect(addr).unwrap();
        let mock_stream_2 = TcpStream::connect(addr).unwrap();
        let mock_stream_3 = TcpStream::connect(addr).unwrap();

        // Create 3 brokers to test the balancing of created partitions
        distribution_manager_lock
            .create_broker(mock_stream_1)
            .unwrap();
        distribution_manager_lock
            .create_broker(mock_stream_2)
            .unwrap();
        distribution_manager_lock
            .create_broker(mock_stream_3)
            .unwrap();

        drop(distribution_manager_lock);

        return distribution_manager;
    }

    #[test]
    fn create_brokers_works_as_expected() {
        let distribution_manager = DistributionManager::new();
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
        let distribution_manager = DistributionManager::new();
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
        // After brokers have connnected to the Observer
        let distribution_manager = setup_distribution_for_tests();
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
        let distribution_manager = setup_distribution_for_tests();
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let topic_name = "notifications";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        let partition_replication_count_1 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_1, brokers_lock.len());

        let partition_replication_count_2 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_2, brokers_lock.len());

        let topic_name = "comments";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'comments'
        let partition_replication_count_3 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_3, brokers_lock.len());

        // Second partition for topic 'comments'
        let partition_replication_count_4 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(partition_replication_count_4, brokers_lock.len());

        let topic_name = "friend_requests";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'friend_requests'
        let partition_replication_count_5 = distribution_manager_lock
            .create_partition("friend_requests")
            .unwrap();

        assert_eq!(partition_replication_count_5, brokers_lock.len());

        // println!("{:#?}", distribution_manager_lock.brokers);
    }
}
