use crate::Role;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Partition {
    id: String,
    replica_id: String,
    role: Role,
}
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Broker {
    host: String,
    partitions: Vec<Partition>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    brokers: Vec<Broker>,
}
