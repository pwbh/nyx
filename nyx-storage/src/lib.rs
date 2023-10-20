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

const BUFFER_MAX_SIZE: usize = 8192;

/// NOTE: Each partition of a topic should have Storage struct
#[derive(Debug)]
pub struct Storage {
    directory: Directory,
    indices: Arc<Mutex<Indices>>,
    file: Arc<File>,
    // TODO: When Storage will be accessed concurrently each concurrent accessor should have a
    // `retrievable_buffer` of its own to read into instead.
    retrivable_buffer: [u8; BUFFER_MAX_SIZE],
    write_sender: Sender<Vec<u8>>,
    write_queue_handle: JoinHandle<Result<(), std::io::Error>>,
}

impl Storage {
    pub async fn new(title: &str, max_queue: usize) -> Result<Self, String> {
        let indices = Indices::new();
        let directory = Directory::new(title).await?;
        let filename = format!("{}.data", title);
        let file = Arc::new(directory.open(&filename).await?);

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
            retrivable_buffer: [0; 8192],
            write_sender,
            write_queue_handle,
        })
    }

    pub fn get_storage_sender(&mut self) -> StorageSender {
        StorageSender::new(self.write_sender.clone())
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
            .map(|v| v.clone())
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

        return Ok(&self.retrivable_buffer[..data_size]);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    async fn simulate_message_send(storage: &mut Storage, message: &str) -> Result<(), String> {
        let mut sender = storage.get_storage_sender();
        sender.send(message.as_bytes()).await
    }

    #[async_std::test]
    async fn create_storage_instance() {
        // (l)eader/(r)eplica_topic-name_partition-count
        let storage = Storage::new("TEST_l_reservations_1", 10_000).await;

        assert!(storage.is_ok());
    }

    #[async_std::test]
    async fn get() {
        let test_message = "hello world";

        let mut storage = Storage::new("TEST_l_reservations_2", 10_000).await.unwrap();
        let send_result = simulate_message_send(&mut storage, test_message).await;

        assert!(send_result.is_ok());

        // wait for the message to arrive to queue
        async_std::task::sleep(Duration::from_millis(15)).await;

        let message = storage.get(0).await;

        assert!(message.is_ok());
        assert_eq!(message.unwrap(), test_message.as_bytes());
    }
}
