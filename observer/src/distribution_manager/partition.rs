use std::sync::{Arc, Mutex};

use super::TopicMetadata;

pub enum ParitionStatus {
    PendingCreation,
    Crashed,
    Active,
}

pub struct PartitionMetadata {
    pub id: String,
    pub status: ParitionStatus,
    pub topic: Arc<Mutex<TopicMetadata>>,
}
