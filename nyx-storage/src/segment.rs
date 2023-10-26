use async_std::{fs, io};

use crate::directory::Directory;

pub enum SegementKind {
    Indices,
    Offsets,
}

pub struct Segment {
    directory: Directory,
    frozen: bool,
    max_size: u16,
    kind: SegementKind,
}

impl Segment {
    pub async fn new(title: &str, kind: SegementKind, max_size: u16) -> io::Result<Self> {
        let directory = Directory::new(title).await?;

        Ok(Self {
            directory,
            max_size,
            frozen: false,
            kind,
        })
    }
}

// Read from 'clean' folder for the user

// Compact files from 'dirty' folder
