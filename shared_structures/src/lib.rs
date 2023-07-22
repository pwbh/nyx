mod topic;

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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Message {
    CreatePartition {
        id: String,
        replica_id: String,
        topic: Topic,
    },
}
