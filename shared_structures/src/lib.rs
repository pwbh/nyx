mod broadcast;
mod dir_manager;
pub mod metadata;
mod topic;

pub use broadcast::Broadcast;
pub use dir_manager::DirManager;
pub use metadata::Metadata;
pub use topic::Topic;

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Status {
    Created,
    Down,
    Up,
    // For replicas that are replicating data from the leader so that they
    // won't be touched by consumers yet until they are Up.
    Booting,
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
        partition_number: usize,
        replica_count: usize,
    },
    RequestLeadership {
        broker_id: String,
        partition_id: String,
        replica_id: String,
    },
    // Should deny leadership request with the addr of broker where leader resides.
    DenyLeadership,
    BrokerWantsToConnect {
        id: String,
        addr: String,
    },
    ProducerWantsToConnect {
        topic: String,
    },
    RequestClusterMetadata,
    ClusterMetadata {
        metadata: Metadata,
    },
    ProducerMessage {
        replica_id: String,
        payload: serde_json::Value,
    },
}

pub fn println_c(text: &str, color: usize) {
    if color > 255 {
        panic!("Color is out of range 0 to 255");
    }

    let t = format!("\x1b[38;5;{}m{}\x1b[0m", color, text);
    println!("{}", t)
}
