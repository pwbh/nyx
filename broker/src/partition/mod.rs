use std::sync::{Arc, Mutex};

use shared_structures::{Role, Status};

use crate::topic::Topic;

mod record;

pub struct Partition {
    pub id: String,
    pub status: Status,
    pub topic: Arc<Mutex<Topic>>,
    pub role: Role,
    partition_number: usize,
    replica_number: usize,
    queue: Vec<record::Record>,
}

impl Partition {
    pub fn from(
        id: String,
        status: Status,
        topic: Topic,
        role: Role,
        partition_number: usize,
        replica_number: usize,
    ) -> Result<Self, String> {
        Ok(Self {
            id,
            status,
            topic: Arc::new(Mutex::new(topic)),
            role,
            partition_number,
            replica_number,
            queue: vec![],
        })
    }
}
