use async_std::{fs::File, io};

use crate::{
    directory::{DataType, Directory},
    MAX_SEGMENT_SIZE,
};

#[derive(Debug)]
pub struct Segment {
    clean: bool,
    length: u64,
    pub file: File,
}

impl Segment {
    pub async fn new(directory: &Directory, data_type: DataType, count: usize) -> io::Result<Self> {
        let file = directory.open_read_write_create(data_type, count).await?;

        Ok(Self {
            length: MAX_SEGMENT_SIZE,
            clean: false,
            file,
        })
    }

    pub async fn from(data_type: DataType, file: File) -> io::Result<Self> {
        Ok(Self {
            length: MAX_SEGMENT_SIZE,
            clean: false,
            file,
        })
    }
}

// Read from 'clean' folder for the user

// Compact files from 'dirty' folder
