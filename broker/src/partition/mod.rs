use std::path::PathBuf;

use shared_structures::{Role, Status, Topic};

use self::partition_db::PartitionDB;

mod partition_db;
mod record;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Partition {
    pub id: String,
    pub replica_id: String,
    pub status: Status,
    pub topic: Topic,
    pub role: Role,
    partition_number: usize,
    replica_number: usize,
    #[serde(skip_serializing, skip_deserializing)]
    database: PartitionDB,
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
        custom_dir: Option<&PathBuf>,
    ) -> Result<Self, String> {
        let database = PartitionDB::with_dir(&replica_id, custom_dir)?;

        Ok(Self {
            id,
            replica_id,
            status,
            topic,
            role,
            partition_number,
            replica_number,
            database,
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
        let custom_dir = PathBuf::from("just_for_test_dir");

        let partition = Partition::from(
            "mocked_partition_id".to_string(),
            "mocked_partition_replica_id".to_string(),
            Status::Up,
            topic,
            Role::Follower,
            1,
            1,
            Some(&custom_dir),
        )
        .unwrap();

        assert_eq!(partition.id, "mocked_partition_id".to_string())
    }
}
