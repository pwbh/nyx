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
        //  for i in 0..self.brokers.len() {
        //      for j in i..self.brokers.len() {
        //          if self.brokers[i].partitions.len() != self.brokers[i].partitions.len() {
        //              check_partitions(&mut self.brokers[i], &mut self.brokers[j])
        //          }
        //      }
        //  }
    }
}

fn check_partitions(current_broker: &mut Broker, other_broker: &mut Broker) {
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
