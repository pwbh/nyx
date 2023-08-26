use crate::{Role, Status, Topic};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PartitionDetails {
    pub id: String,
    pub replica_id: String,
    pub role: Role,
    pub topic: Topic,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BrokerDetails {
    pub addr: String,
    pub status: Status,
    pub partitions: Vec<PartitionDetails>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub brokers: Vec<BrokerDetails>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self { brokers: vec![] }
    }
}
