use std::sync::Arc;

use super::{topic::Topic, Broker, Role};

#[derive(Clone, Copy)]
pub enum Status {
    PendingCreation,
    Crashed,
    Active,
}

#[derive(Clone)]
pub struct Partition {
    pub id: String,
    pub status: Status,
    pub broker: Option<Arc<Broker>>,
    pub topic: Arc<Topic>,
    pub role: Role,
}

impl Partition {
    pub fn new(topic: &Arc<Topic>) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            status: Status::PendingCreation,
            topic: topic.clone(),
            broker: None,
            role: Role::Follower,
        }
    }
}
