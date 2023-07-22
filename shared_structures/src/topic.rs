use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Topic {
    pub name: String,
    pub partition_count: usize,
}

impl Topic {
    pub fn from(name: String) -> Self {
        Self {
            name,
            partition_count: 0,
        }
    }

    pub fn new_shared(name: String) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            name,
            partition_count: 0,
        }))
    }
}
