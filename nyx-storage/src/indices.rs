use std::{collections::HashMap, sync::Arc};

use async_std::sync::Mutex;

use crate::offsets::Offsets;

#[derive(Debug)]
pub struct Indices {
    pub data: HashMap<usize, Offsets>,
    pub length: usize,
    pub total_bytes: usize,
}

impl Indices {
    pub fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            data: HashMap::new(),
            length: 0,
            total_bytes: 0,
        }))
    }
}
