use std::sync::{Arc, Mutex};

use shared_structures::{Role, Status, Topic};

mod partition_db;
mod record;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Partition {
    pub id: String,
    pub replica_id: String,
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
        replica_id: String,
        status: Status,
        topic: Topic,
        role: Role,
        partition_number: usize,
        replica_number: usize,
    ) -> Result<Self, String> {
        //let database = partition_db::

        Ok(Self {
            id,
            replica_id,
            status,
            topic: Arc::new(Mutex::new(topic)),
            role,
            partition_number,
            replica_number,
            queue: vec![],
        })
    }

    // pub fn send_candidacy_for_leadership(&self, observer: &TcpStream) -> Result<()> {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_partition_on_broker() {
        let topic = Topic::from("notifications".to_string());

        let partition = Partition::from(
            "mocked_partition_id".to_string(),
            "mocked_partition_replica_id".to_string(),
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
