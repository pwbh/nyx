use std::path::PathBuf;

use shared_structures::{Role, Status, Topic};

use self::partition_db::PartitionDB;

mod partition_db;
mod record;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PartitionDetails {
    pub id: String,
    pub replica_id: String,
    pub status: Status,
    pub topic: Topic,
    pub role: Role,
    pub partition_number: usize,
    pub replica_number: usize,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Partition {
    details: PartitionDetails,
    #[serde(skip_serializing, skip_deserializing)]
    pub database: PartitionDB,
}

impl Partition {
    pub fn from(details: PartitionDetails, custom_dir: Option<&PathBuf>) -> Result<Self, String> {
        let database = PartitionDB::with_dir(&details.replica_id, custom_dir)?;

        println!("Database initialized");

        Ok(Self { details, database })
    }

    // pub fn send_candidacy_for_leadership(&self, observer: &TcpStream) -> Result<()> {}
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn creates_partition_on_broker() {
        let topic = Topic::from("notifications".to_string());
        let custom_dir = PathBuf::from("just_for_test_dir");

        let partition_info = PartitionDetails {
            id: "mocked_partition_id".to_string(),
            replica_id: "mocked_partition_replica_id".to_string(),
            status: Status::Up,
            topic,
            role: Role::Follower,
            partition_number: 1,
            replica_number: 1,
        };

        let partition = Partition::from(partition_info, Some(&custom_dir)).unwrap();

        assert_eq!(partition.details.id, "mocked_partition_id".to_string())
    }
}
