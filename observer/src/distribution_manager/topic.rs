use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct Topic {
    pub name: String,
    pub partition_count: usize,
}

impl Topic {
    pub fn new(name: String) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            name,
            partition_count: 0,
        }))
    }
}
