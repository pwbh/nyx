use async_std::{fs::File, io};

use crate::directory::{DataType, Directory};

#[derive(Debug)]
pub struct Segment {
    clean: bool,
    length: u64,
    pub file: File,
    pub location: String,
}

impl Segment {
    pub async fn new(
        directory: &Directory,
        data_type: DataType,
        length: u64,
        count: usize,
    ) -> io::Result<Self> {
        let file = directory.open_read_write(data_type, count).await?;
        let location = directory.get_file_path(data_type, count)?;

        Ok(Self {
            length,
            clean: false,
            file,
            location,
        })
    }
}

// Read from 'clean' folder for the user

// Compact files from 'dirty' folder
