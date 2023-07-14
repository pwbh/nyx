#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum Status {
    Created,
    Crashed,
    Active,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum Role {
    Follower,
    Leader,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Message<T: serde::Serialize> {
    CreatePartition(T),
}
