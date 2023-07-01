use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
};

mod broker;
mod partition;
mod topic;

pub use broker::Broker;

use self::{
    partition::{Partition, Status},
    topic::Topic,
};

#[derive(Clone, Copy)]
pub enum Role {
    Follower,
    Leader,
}

pub struct DistributionManager {
    brokers: Vec<Broker>,
    topics: Vec<Arc<Topic>>,
}

impl DistributionManager {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            brokers: vec![],
            topics: vec![],
        }))
    }

    // Need to rebalance if new broker is added
    pub fn create_broker(&mut self, stream: TcpStream) -> Result<(), String> {
        let broker = Broker::from(stream)?;
        self.brokers.push(broker);
        self.rebalance();
        Ok(())
    }

    pub fn create_topic(&mut self, topic_name: &str) -> Result<(), String> {
        if self.brokers.len() == 0 {
            return Err("0 brokers have been found, please add a broker.".to_string());
        }

        let topic_exists = self.topics.iter().any(|t| t.name == *topic_name);

        if topic_exists {
            return Err(format!("Topic `{}` already exist.", topic_name));
        }

        let topic = Topic::new(topic_name.to_string());

        self.topics.push(topic);

        Ok(())
    }

    // Need to rebalance if new partition is added to the broker
    pub fn create_partition(&mut self, topic_name: &str) -> Result<(), String> {
        if self.brokers.len() == 0 {
            return Err("0 brokers have been found, please add a broker.".to_string());
        }

        let topic = self.topics.iter().find(|t| t.name == *topic_name);

        if let Some(topic) = topic {
            let partition = Partition::new(&topic);
            self.brokers.iter_mut().for_each(|b| {
                b.partitions.push(partition.clone());
            });
            // begin leadership race among topic's partitions
        } else {
            return Err(format!("Topic `{}` doesn't exist.", topic_name));
        }

        Ok(())
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
            other_broker.partitions.push(fresh_partition);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::TcpListener, time::Duration};

    use super::*;

    #[test]
    fn partition_distribution_works_as_exepcted() {
        let distribution_manager = DistributionManager::new();
        let mut distribution_manager_lock = distribution_manager.lock().unwrap();

        let addr = "localhost:3000";
        let listener = TcpListener::bind(addr).unwrap();

        std::thread::spawn(move || loop {
            listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(150))
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

        let topic_name = "test_topic";

        distribution_manager_lock.create_topic(topic_name).unwrap();

        let result = distribution_manager_lock.create_partition(topic_name);

        assert!(result.is_ok());

        for broker in distribution_manager_lock.brokers.iter() {
            assert_eq!(broker.partitions.len(), 1);
        }
    }
}
