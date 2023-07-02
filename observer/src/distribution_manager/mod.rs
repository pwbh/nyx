use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
};

mod broker;
mod partition;
mod topic;

pub use broker::Broker;
use uuid::Uuid;

use self::{
    partition::{Partition, Status},
    topic::Topic,
};

#[derive(Clone, Copy, Debug)]
pub enum Role {
    Follower,
    Leader,
}

#[derive(Debug)]
pub struct DistributionManager {
    brokers: Vec<Broker>,
    topics: Vec<Arc<Mutex<Topic>>>,
}

impl DistributionManager {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            brokers: vec![],
            topics: vec![],
        }))
    }

    // Need to rebalance if new broker is added
    pub fn create_broker(&mut self, stream: TcpStream) -> Result<&Broker, String> {
        let broker = Broker::from(stream)?;
        self.brokers.push(broker);
        self.rebalance();
        Ok(self.brokers.last().unwrap())
    }

    pub fn create_topic(&mut self, topic_name: &str) -> Result<&Arc<Mutex<Topic>>, String> {
        if self.brokers.len() == 0 {
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

        Ok(self.topics.last().unwrap())
    }

    // Need to rebalance if new partition is added to the broker
    pub fn create_partition(&mut self, topic_name: &str) -> Result<usize, String> {
        if self.brokers.len() == 0 {
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

            self.brokers.iter_mut().for_each(|b| {
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

    fn rebalance(&mut self) {
        for i in 0..self.brokers.len() {
            let (a, b) = self.brokers.split_at_mut(i + 1);

            let current_broker = &mut a[i];

            for other_broker in b.iter_mut() {
                balance_brokers(current_broker, other_broker);
            }
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
            let mut fresh_partition = current_partition.clone();
            fresh_partition.status = Status::PendingCreation;
            fresh_partition.id = Uuid::new_v4().to_string();
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

        assert_eq!(
            partition_replication_count_1,
            distribution_manager_lock.brokers.len()
        );

        let partition_replication_count_2 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_2,
            distribution_manager_lock.brokers.len()
        );

        let topic_name = "comments";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'comments'
        let partition_replication_count_3 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_3,
            distribution_manager_lock.brokers.len()
        );

        // Second partition for topic 'comments'
        let partition_replication_count_4 = distribution_manager_lock
            .create_partition(topic_name)
            .unwrap();

        assert_eq!(
            partition_replication_count_4,
            distribution_manager_lock.brokers.len()
        );

        let topic_name = "friend_requests";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        // First partition for topic 'friend_requests'
        let partition_replication_count_5 = distribution_manager_lock
            .create_partition("friend_requests")
            .unwrap();

        assert_eq!(
            partition_replication_count_5,
            distribution_manager_lock.brokers.len()
        );

        // println!("{:#?}", distribution_manager_lock.brokers);
    }
}
