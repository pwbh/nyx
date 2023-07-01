use std::sync::Arc;

#[derive(Clone)]
pub struct Topic {
    pub name: String,
    pub partition_count: usize,
}

impl Topic {
    pub fn new(name: String) -> Arc<Self> {
        Arc::new(Self {
            name,
            partition_count: 0,
        })
    }
}
