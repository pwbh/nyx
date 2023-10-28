use std::{io::Error, sync::Arc};

use async_std::{
    channel::{bounded, Sender},
    fs::File,
    io::{prelude::SeekExt, ReadExt, SeekFrom, WriteExt},
    sync::Mutex,
    task::JoinHandle,
};
use compactor::Compactor;
use directory::{DataType, Directory};
use indices::Indices;
use offset::Offset;
use segment::Segment;
use segmentation_manager::SegmentationManager;
use storage_sender::StorageSender;
use write_queue::WriteQueue;

mod compactor;
mod indices;
mod macros;
mod offset;
mod segment;
mod segmentation_manager;
mod storage_sender;
mod write_queue;

pub mod directory;

const MAX_BUFFER_SIZE: usize = 4096;
const MAX_SEGMENT_SIZE: u64 = 4_000_000_000;

/// NOTE: Each partition of a topic should have a Storage for the data it stores
#[derive(Debug)]
pub struct Storage {
    pub directory: Directory,
    indices: Arc<Mutex<Indices>>,
    segmentation_manager: Arc<Mutex<SegmentationManager>>,
    retrivable_buffer: [u8; MAX_BUFFER_SIZE],
    write_sender: Sender<Vec<u8>>,
    segment_sender: Sender<Segment>,
    write_queue_handle: JoinHandle<Result<(), std::io::Error>>,
    compaction: bool,
}

impl Storage {
    pub async fn new(title: &str, max_queue: usize, compaction: bool) -> Result<Self, String> {
        let directory = Directory::new(title)
            .await
            .map_err(|e| format!("Storage (Directory::new): {}", e))?;

        let indices = Indices::from(&directory)
            .await
            .map_err(|e| format!("Storage (Indices::from): {}", e))?;

        let segmentation_manager = SegmentationManager::new(&directory)
            .await
            .map_err(|e| format!("Storage (SegmentationManager::new): {}", e))?;

        let (write_sender, write_receiver) = bounded(max_queue);
        let (segment_sender, segment_receiver) = bounded(max_queue);

        if compaction {
            async_std::task::spawn(Compactor::run(segment_receiver));
        }

        let write_queue_handle = async_std::task::spawn(WriteQueue::run(
            indices.clone(),
            segmentation_manager.clone(),
            write_receiver,
        ));

        Ok(Self {
            directory,
            indices,
            segmentation_manager,
            retrivable_buffer: [0; MAX_BUFFER_SIZE],
            write_sender,
            segment_sender,
            write_queue_handle,
            compaction,
        })
    }

    pub fn get_storage_sender(&mut self) -> StorageSender {
        StorageSender::new(self.write_sender.clone())
    }

    pub async fn set(&mut self, data: &[u8]) -> Result<(), String> {
        self.append(data)
            .await
            .map_err(|e| format!("Failed to send data: {}", e))?;
        Ok(())
    }

    async fn append(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut segmentation_manager = self.segmentation_manager.lock().await;

        if buf.len() > MAX_BUFFER_SIZE {
            return Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "buf size: {} kb maximum buffer size {} kb",
                    buf.len(),
                    MAX_BUFFER_SIZE
                ),
            ));
        }

        let (latest_segment_count, latest_partition_segment) = segmentation_manager
            .get_latest_segment(DataType::Partition)
            .await?;

        let mut latest_partition_file = &latest_partition_segment.data;

        latest_partition_file.write_all(buf).await?;

        let mut indices = self.indices.lock().await;

        let length = indices.length;
        let total_bytes = indices.total_bytes;

        let offset = Offset::new(total_bytes, total_bytes + buf.len(), latest_segment_count)
            .map_err(|e: String| Error::new(std::io::ErrorKind::InvalidData, e))?;

        indices.data.insert(length, offset);
        indices.length += 1;
        indices.total_bytes += buf.len();

        drop(indices);

        let index_bytes = unsafe { *(&length as *const _ as *const [u8; 8]) };
        let offset = offset.as_bytes();

        let (_, latest_indices_segment) = segmentation_manager
            .get_latest_segment(DataType::Indices)
            .await?;
        let mut latest_indices_file = &latest_indices_segment.data;

        latest_indices_file.write_all(&index_bytes).await?;
        latest_indices_file.write_all(offset).await?;

        Ok(buf.len())
    }

    pub async fn len(&self) -> usize {
        let indices: async_std::sync::MutexGuard<'_, Indices> = self.indices.lock().await;
        indices.data.len()
    }

    pub async fn get(&mut self, index: usize) -> Option<&[u8]> {
        // TODO: Think of a better way to do this, maybe able to get rid of the lock somehow
        // and still be safe.
        let indices = self.indices.lock().await;

        let offsets = indices.data.get(&index).cloned()?;

        drop(indices);

        let data_size = offsets.data_size();

        let mut segment_data = self
            .directory
            .open_read(DataType::Partition, offsets.segment_index())
            .await
            .ok()?;

        self.seek_bytes_between(offsets.start(), data_size, &mut segment_data)
            .await
    }

    async fn seek_bytes_between(
        &mut self,
        start: usize,
        data_size: usize,
        data: &mut File,
    ) -> Option<&[u8]> {
        data.seek(SeekFrom::Start(start as u64)).await.ok()?;
        data.read(&mut self.retrivable_buffer[..data_size])
            .await
            .ok()?;
        Some(&self.retrivable_buffer[..data_size])
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
        let mut storage = Storage::new(title, 10_000, false).await.unwrap();

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
        let storage = Storage::new("TEST_l_reservations_1", 10_000, false).await;

        assert!(storage.is_ok());
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn get_returns_ok() {
        let message_count = 1_000;

        let test_message = b"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcd";

        let mut storage = setup_test_storage(&function!(), test_message, message_count, 1600).await;

        let indices = storage.indices.lock().await;
        let length = indices.length;
        drop(indices);

        let now = Instant::now();

        for index in 0..length {
            let message = storage.get(index).await;

            assert!(message.is_some());
            assert_eq!(message.unwrap(), test_message);
        }

        let elapsed = now.elapsed();

        println!("Read {} messages in: {:.2?}", length, elapsed);

        let indices = storage.indices.lock().await;
        let length = indices.length;
        drop(indices);

        assert_eq!(length, message_count);

        //  cleanup(&storage).await;
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn get_returns_err_on_index_out_of_bounds() {
        let total_count = 5;

        let test_message = b"hello world hello world hello worldrld hello worldrld hello worl";

        let mut storage = setup_test_storage(&function!(), test_message, total_count, 5).await;

        let get_result = storage.get(total_count).await;

        assert_eq!(get_result, None);

        cleanup(&storage).await;
    }
}
