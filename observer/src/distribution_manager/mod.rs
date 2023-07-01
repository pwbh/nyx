use std::{
    cell::RefCell,
    net::TcpStream,
    rc::Rc,
    sync::{Arc, Mutex},
};

use uuid::Uuid;

mod broker;
mod partition;
mod topic;

pub use broker::Broker;
pub use partition::{ParitionStatus, PartitionMetadata};
pub use topic::TopicMetadata;

pub struct DistributionManager {
    brokers: Vec<Broker>,
    topics: Vec<Arc<Mutex<TopicMetadata>>>,
    partitions: Vec<PartitionMetadata>,
}

impl DistributionManager {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            brokers: vec![],
            topics: vec![],
            partitions: vec![],
        }))
    }

    pub fn create_broker(&mut self, stream: TcpStream) -> Result<(), String> {
        let broker = Broker::from(stream)?;

        self.brokers.push(broker);
        Ok(())
    }

    pub fn create_topic(&mut self, topic_name: &str) -> Result<(), String> {
        let topic_exists = self.topics.iter().any(|t| {
            let t = t.lock().unwrap();
            t.name == *topic_name
        });

        if topic_exists {
            return Err(format!("Topic `{}` already exist.", topic_name));
        }

        let topic = TopicMetadata {
            name: topic_name.to_string(),
            current_host: None,
            partition_count: 0,
        };

        self.topics.push(Arc::new(Mutex::new(topic)));

        Ok(())
    }

    pub fn craete_partition(&mut self, topic_name: &str) -> Result<(), String> {
        let topic = self.topics.iter().find(|t| {
            let t = t.lock().unwrap();
            t.name == *topic_name
        });

        if let Some(topic) = topic {
            let partition = PartitionMetadata {
                id: Uuid::new_v4().to_string(),
                status: ParitionStatus::PendingCreation,
                topic: topic.clone(),
            };

            self.partitions.push(partition);
        } else {
            return Err(format!("Topic `{}` doesn't exist.", topic_name));
        }

        Ok(())
    }
}
