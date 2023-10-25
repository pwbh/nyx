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

/// NOTE: Each partition of a topic should have a Storage for the data it stores
#[derive(Debug)]
pub struct Storage {
    pub directory: Directory,
    indices: Arc<Mutex<Indices>>,
    // Partition data in read mode only
    data: File,
    // TODO: When Storage will be accessed concurrently each concurrent accessor (consumer) should have a
    // `retrievable_buffer` of its own to read into instead.
    retrivable_buffer: [u8; BUFFER_MAX_SIZE],
    write_sender: Sender<Vec<u8>>,
    pub write_queue_handle: JoinHandle<Result<(), std::io::Error>>,
}

impl Storage {
    pub async fn new(title: &str, max_queue: usize) -> Result<Self, String> {
        let directory = Directory::new(title)
            .await
            .map_err(|e| format!("Storage (directory): {}", e))?;

        directory
            .create_all()
            .await
            .map_err(|e| format!("String (create_all): {}", e))?;

        let indices = Indices::from(&directory)
            .await
            .map_err(|e| format!("Storage (Indices::from): {}", e))?;

        let data: File = directory
            .open_read(&directory::DataType::Partition)
            .await
            .map_err(|e| format!("Storage (open_read): {}", e))?;

        let (write_sender, write_receiver) = bounded(max_queue);

        let write_queue_handle = async_std::task::spawn(WriteQueue::run(
            indices.clone(),
            write_receiver,
            directory.clone(),
        ));

        Ok(Self {
            directory,
            indices,
            data,
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
        let indices: async_std::sync::MutexGuard<'_, Indices> = self.indices.lock().await;
        indices.data.len()
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
                "Seeked data size: {} kb maximum retrivable size {} kb",
                data_size,
                self.retrivable_buffer.len()
            ));
        }

        self.seek_bytes_between(offsets.start(), data_size)
            .await
            .map_err(|e| format!("Error in Storage (get): {}", e))
    }

    async fn seek_bytes_between(&mut self, start: usize, data_size: usize) -> io::Result<&[u8]> {
        self.data.seek(SeekFrom::Start(start as u64)).await?;

        self.data
            .read(&mut self.retrivable_buffer[..data_size])
            .await?;

        Ok(&self.retrivable_buffer[..data_size])
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::macros::function;

    use super::*;

    async fn cleanup(storage: &Storage) {
        storage.directory.delete_all().await.unwrap();
    }

    async fn setup_test_storage(
        title: &str,
        test_message: &[u8],
        count: usize,
        wait_ms: u64,
    ) -> Storage {
        let mut storage = Storage::new(title, 10_000).await.unwrap();

        let messages = vec![test_message; count];

        let now = Instant::now();

        for message in messages {
            storage.set(message).await.unwrap();
        }

        let elapsed = now.elapsed();

        println!("Write {} messages in: {:.2?}", count, elapsed);

        // wait for the message to arrive from the queue
        async_std::task::sleep(Duration::from_millis(wait_ms)).await;

        assert_eq!(storage.len().await, count);

        return storage;
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
    async fn get_returns_ok() {
        let test_message = b"hello world hello world hello worldrld hello worldrld hello worl";

        let mut storage = setup_test_storage(&function!(), test_message, 1_000, 100).await;

        let indices = storage.indices.lock().await;
        let length = indices.length;
        drop(indices);

        let now = Instant::now();

        for index in 0..length {
            let message = storage.get(index).await;

            assert!(message.is_ok());
            assert_eq!(message.unwrap(), test_message);
        }

        let elapsed = now.elapsed();

        println!("Read {} messages in: {:.2?}", length, elapsed);

        cleanup(&storage).await;
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn get_returns_err_on_index_out_of_bounds() {
        let total_count = 5;

        let test_message = b"hello world hello world hello worldrld hello worldrld hello worl";

        let mut storage = setup_test_storage(&function!(), test_message, total_count, 5).await;

        let get_result = storage.get(total_count).await;

        assert!(get_result.is_err());

        cleanup(&storage).await;
    }
}
