use std::sync::{Arc, Mutex};

use shared_structures::{Role, Status};

use crate::topic::Topic;

mod record;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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

#[cfg(test)]
mod tests {

    use crate::topic;

    use super::*;

    #[test]
    fn creates_partition_on_broker() {
        let topic =
            topic::Topic::from("mock_topic_id".to_string(), "notifications".to_string()).unwrap();

        let partition = Partition::from(
            "mocked_partition_id".to_string(),
            Status::Up,
            topic,
            Role::Follower,
            1,
            1,
        )
        .unwrap();

        assert_eq!(partition.id, "mocked_partition_id".to_string())
    }
}
