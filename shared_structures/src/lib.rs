mod broadcast;
pub mod metadata;
mod topic;

pub use broadcast::Broadcast;
pub use metadata::Metadata;
pub use topic::Topic;

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Status {
    Created,
    Down,
    Up,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum Role {
    Follower,
    Leader,
}

// TODO: Think of a way to better organize this enum
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Message {
    CreatePartition {
        id: String,
        replica_id: String,
        topic: Topic,
    },
    RequestLeadership {
        broker_id: String,
        partition_id: String,
        replica_id: String,
    },
    DenyLeadership,
    BrokerWantsToConnect {
        id: String,
    },
    ProducerWantsToConnect {
        topic: String,
    },
    RequestClusterMetadata,
    ClusterMetadata {
        metadata: Metadata,
    },
}
