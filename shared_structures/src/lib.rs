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

#[derive(PartialEq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Message<T: serde::Serialize> {
    CreatePartition(T),
    Any(T),
}
