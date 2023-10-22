use std::{collections::HashMap, sync::Arc};

use async_std::{io, sync::Mutex};

use crate::{directory::Directory, offsets::Offsets};

#[derive(Debug)]
pub struct Indices {
    pub data: HashMap<usize, Offsets>,
    pub length: usize,
    pub total_bytes: usize,
}

impl Indices {
    pub fn new() -> Arc<Mutex<Self>> {
        let indices = Self {
            data: HashMap::new(),
            length: 0,
            total_bytes: 0,
        };

        Arc::new(Mutex::new(indices))
    }

    async fn load_from_disk(directory: &Directory) -> io::Result<()> {
        let disk_data = directory
            .open_write(&crate::directory::DataType::Indices)
            .await?;

        unimplemented!()
    }

    async fn set(index: usize, offsets: &Offsets) -> io::Result<()> {
        unimplemented!()
    }
}
