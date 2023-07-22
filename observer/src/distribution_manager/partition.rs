use std::sync::{Arc, Mutex};

use shared_structures::{Role, Status, Topic};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Partition {
    pub id: String,
    pub replica_id: String,
    pub status: Status,
    pub topic: Arc<Mutex<Topic>>,
    pub role: Role,
    partition_number: usize,
    pub replica_count: usize,
}

impl Partition {
    pub fn new(topic: &Arc<Mutex<Topic>>, partition_number: usize) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            replica_id: uuid::Uuid::new_v4().to_string(),
            status: Status::Created,
            topic: topic.clone(),
            role: Role::Follower,
            partition_number,
            replica_count: 0,
        }
    }

    pub fn replicate(partition: &Self, replica_count: usize) -> Self {
        Self {
            replica_id: uuid::Uuid::new_v4().to_string(),
            replica_count,
            ..partition.clone()
        }
    }
}
