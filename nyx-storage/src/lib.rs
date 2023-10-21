use std::{
    io::{self},
    sync::Arc,
};

use async_std::{
    channel::{bounded, Sender},
    fs::File,
    io::{prelude::SeekExt, ReadExt, SeekFrom},
    sync::Mutex,
    task::JoinHandle,
};
use directory::Directory;
use indices::Indices;
use storage_sender::StorageSender;
use write_queue::WriteQueue;

mod indices;
mod macros;
mod offsets;
mod storage_sender;
mod write_queue;

pub mod directory;

const BUFFER_MAX_SIZE: usize = 4096;

/// NOTE: Each partition of a topic should have Storage struct
#[derive(Debug)]
pub struct Storage {
    pub directory: Directory,
    indices: Arc<Mutex<Indices>>,
    file: Arc<File>,
    // TODO: When Storage will be accessed concurrently each concurrent accessor should have a
    // `retrievable_buffer` of its own to read into instead.
    retrivable_buffer: [u8; BUFFER_MAX_SIZE],
    write_sender: Sender<Vec<u8>>,
    pub write_queue_handle: JoinHandle<Result<(), std::io::Error>>,
}

impl Storage {
    pub async fn new(title: &str, max_queue: usize) -> Result<Self, String> {
        let indices = Indices::new();
        let directory = Directory::new(title).await?;
        let file = Arc::new(directory.open().await?);

        let (write_sender, write_receiver) = bounded(max_queue);

        let write_queue_handle = async_std::task::spawn(WriteQueue::run(
            indices.clone(),
            write_receiver,
            file.clone(),
        ));

        Ok(Self {
            directory,
            indices,
            file,
            retrivable_buffer: [0; BUFFER_MAX_SIZE],
            write_sender,
            write_queue_handle,
        })
    }

    pub fn get_storage_sender(&mut self) -> StorageSender {
        StorageSender::new(self.write_sender.clone())
    }

    pub async fn set(&mut self, data: &[u8]) -> Result<(), String> {
        self.write_sender
            .send(data.to_vec())
            .await
            .map_err(|e| format!("Failed to send data: {}", e))
    }

    pub async fn len(&self) -> usize {
        let indices = self.indices.lock().await;
        indices.length
    }

    pub async fn get(&mut self, index: usize) -> Result<&[u8], String> {
        // TODO: Think of a better way to do this, maybe able to get rid of the lock somehow
        // and still be safe.
        let indices = self.indices.lock().await;
        let length = indices.length;

        if index >= length {
            return Err(format!("Out of bounds requested data at index {}", index));
        }

        let offsets = indices
            .data
            .get(&index)
            .copied()
            .ok_or("record doesn't exist.".to_string())?;

        drop(indices);

        let data_size = offsets.end() - offsets.start();

        if data_size > self.retrivable_buffer.len() {
            return Err(format!(
                "Seeked data size: {}kb maximum retrivable size {}kb",
                data_size,
                self.retrivable_buffer.len()
            ));
        }

        self.seek_bytes_between(offsets.start(), data_size)
            .await
            .map_err(|e| format!("Error in Storage (get): {}", e))
    }

    async fn seek_bytes_between(&mut self, start: usize, data_size: usize) -> io::Result<&[u8]> {
        let mut file = &*self.file;
        file.seek(SeekFrom::Start(start as u64)).await?;
        let n: usize = file.read(&mut self.retrivable_buffer[..data_size]).await?;
        if n == 0 {
            // Theoratically should never get here
            panic!(
                "Got 0 bytes in file read start: {} data_size: {} ",
                start, data_size
            )
        };

        Ok(&self.retrivable_buffer[..data_size])
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn cleanup(storage: &Storage) {
        storage.directory.remove().await.unwrap();
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn new_creates_instances() {
        // (l)eader/(r)eplica_topic-name_partition-count
        let storage = Storage::new("TEST_l_reservations_1", 10_000).await;

        assert!(storage.is_ok());
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn get_gets_data_from_storage() {
        let test_message = b"hello world this is a longer string which we are testing to test";

        let mut storage = Storage::new("TEST_l_reservations_2", 10_000).await.unwrap();

        let count = 1_000;

        let messages = vec![test_message; count];

        for message in messages {
            storage.set(message).await.unwrap();
        }

        // wait for the message to arrive from the queue
        async_std::task::sleep(Duration::from_millis(50)).await;

        println!("Indices length: {}", storage.len().await);

        assert_eq!(storage.len().await, count);

        for index in 0..count {
            let message = storage.get(index).await;

            assert!(message.is_ok());
            assert_eq!(message.unwrap(), test_message);
        }

        cleanup(&storage).await;
    }
}
