use crate::{Role, Status};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PartitionDetails {
    pub id: String,
    pub replica_id: String,
    pub role: Role,
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BrokerDetails {
    pub host: String,
    pub status: Status,
    pub partitions: Vec<PartitionDetails>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub brokers: Vec<BrokerDetails>,
}
