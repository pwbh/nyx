use std::{
    collections::HashMap,
    io::{self},
    sync::Arc,
};

use async_std::{
    channel::{bounded, Sender},
    fs::File,
    io::{prelude::SeekExt, ReadExt, SeekFrom},
    task::JoinHandle,
};
use directory::Directory;
use storage_sender::StorageSender;
use write_queue::WriteQueue;

mod offsets;
mod storage_sender;
mod write_queue;

pub mod directory;

use offsets::Offsets;

const BUFFER_MAX_SIZE: usize = 8192;

/// NOTE: Each partition of a topic should have Storage struct
pub struct Storage {
    directory: Directory,
    indices: HashMap<usize, Offsets>,
    max_index: usize,
    file: Arc<File>,
    // TODO: When Storage will be accessed concurrently each concurrent accessor should have a
    // `retrievable_buffer` of its own to read into instead.
    retrivable_buffer: [u8; BUFFER_MAX_SIZE],
    write_sender: Sender<Vec<u8>>,
    write_queue_handle: JoinHandle<Result<(), std::io::Error>>,
}

impl Storage {
    pub async fn new(title: &str, max_queue: usize) -> Result<Self, String> {
        let directory = Directory::new(title).await?;
        let file = Arc::new(directory.open(title).await?);
        let (write_sender, write_receiver) = bounded(max_queue);

        let write_queue_handle =
            async_std::task::spawn(WriteQueue::run(write_receiver, file.clone()));

        Ok(Self {
            directory,
            indices: HashMap::new(),
            max_index: 0,
            file,
            retrivable_buffer: [0; 8192],
            write_sender,
            write_queue_handle,
        })
    }

    pub fn get_storage_sender(&mut self) -> StorageSender {
        StorageSender::new(self.write_sender.clone())
    }

    pub async fn get(&mut self, index: usize) -> Result<&[u8], String> {
        if index >= self.max_index {
            return Err(format!(
                "Request data at index {} but max index is {}",
                index,
                self.max_index - 1
            ));
        }

        let offsets = self
            .indices
            .get(&index)
            .ok_or("record doesn't exist.".to_string())?;

        let data_size = offsets.end() - offsets.start();

        if data_size > self.retrivable_buffer.len() {
            return Err(format!(
                "Seeked data size: {}kb maximum retrivable size {}kb",
                data_size,
                self.retrivable_buffer.len()
            ));
        }

        return self
            .seek_bytes_between(offsets.start(), data_size)
            .await
            .map_err(|e| format!("Error in Storage (get): {}", e));
    }

    async fn seek_bytes_between(&mut self, start: usize, data_size: usize) -> io::Result<&[u8]> {
        let mut file = &*self.file;
        file.seek(SeekFrom::Start(start as u64)).await?;
        let n = file.read(&mut self.retrivable_buffer[..data_size]).await?;
        if n == 0 {
            // Theoratically should never get here
            panic!("Got 0 bytes in file read")
        };
        self.rewind().await?;
        return Ok(&self.retrivable_buffer[..data_size]);
    }

    async fn rewind(&mut self) -> io::Result<u64> {
        let mut file = &*self.file;
        file.seek(SeekFrom::Start(0)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_storage_instance() {
        //   let storage = Storage::new();
    }
}
