use uuid::Uuid;

mod partition;

pub struct Topic {
    id: String,
    name: String,
    partitions: Vec<partition::Partition>,
}

impl Topic {
    pub fn new(name: String, partition_count: usize) -> Result<Self, String> {
        Ok(Self {
            id: Uuid::new_v4().to_string(),
            name,
            partitions: vec![],
        })
    }
}
