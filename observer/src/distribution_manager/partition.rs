use std::sync::{Arc, Mutex};

use super::{topic::Topic, Role};

#[derive(Clone, Copy, Debug)]
pub enum Status {
    PendingCreation,
    Crashed,
    Active,
}

#[derive(Clone, Debug)]
pub struct Partition {
    pub id: String,
    pub status: Status,
    pub topic: Arc<Mutex<Topic>>,
    pub role: Role,
    partition_number: usize,
    replica_number: usize,
}

impl Partition {
    pub fn new(topic: &Arc<Mutex<Topic>>, partition_number: usize) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            status: Status::PendingCreation,
            topic: topic.clone(),
            role: Role::Follower,
            partition_number,
            replica_number: 0,
        }
    }

    pub fn replicate(partition: &Self, replica_number: usize) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            replica_number,
            ..partition.clone()
        }
    }
}
