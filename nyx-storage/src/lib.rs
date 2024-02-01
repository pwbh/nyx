use std::sync::Arc;

use async_std::io::{self, prelude::SeekExt, ReadExt, SeekFrom, WriteExt};
use batch::{Batch, BatchState};
use directory::{DataType, Directory};
use indices::Indices;
use segment::Segment;
use segmentation_manager::SegmentationManager;

mod batch;
mod compactor;
mod indices;
mod macros;
mod offset;
mod segment;
mod segmentation_manager;
mod storage_sender;

pub mod directory;

// 4KB
const MAX_MESSAGE_SIZE: usize = 4096;
// 4GB
const MAX_SEGMENT_SIZE: u64 = 4_000_000_000;
// 16KB
const MAX_BATCH_SIZE: usize = 16384;

/// NOTE: Each partition of a topic should have a Storage for the data it stores
#[derive(Debug)]
pub struct Storage {
    pub directory: Directory,
    indices: Indices,
    segmentation_manager: SegmentationManager,
    retrivable_buffer: [u8; MAX_MESSAGE_SIZE],
    batch: Batch,
    compaction: bool,
}

impl Storage {
    pub async fn new(title: &str, compaction: bool) -> Result<Self, String> {
        let directory = Directory::new(title)
            .await
            .map_err(|e| format!("Storage (Directory::new): {}", e))?;

        let segmentation_manager = SegmentationManager::from(&directory)
            .await
            .map_err(|e| format!("Storage (SegmentationManager::from): {}", e))?;

        let indices = Indices::from(segmentation_manager.indices_segments())
            .await
            .map_err(|e| format!("Storage (Indices::from): {}", e))?;

        println!("{:#?}", indices);

        if compaction {
            // async_std::task::spawn(Compactor::run(segment_receiver));
        }

        Ok(Self {
            directory,
            indices,
            segmentation_manager,
            retrivable_buffer: [0; MAX_MESSAGE_SIZE],
            batch: Batch::new(),
            compaction,
        })
    }

    pub async fn set(&mut self, buf: &[u8]) -> Result<(), String> {
        if buf.len() > MAX_MESSAGE_SIZE {
            return Err(format!(
                "Payload size {} kb, max payload allowed {} kb",
                buf.len(),
                MAX_MESSAGE_SIZE
            ));
        }

        let latest_segment_count = self
            .segmentation_manager
            .get_last_segment_count(DataType::Partition);

        let latest_segment_size = self
            .segmentation_manager
            .get_last_segment_size(DataType::Partition)
            .await;

        let last_total_entries = self.len();

        let batch_state = self.batch.add(
            buf,
            latest_segment_count,
            latest_segment_size,
            last_total_entries,
        )?;

        if batch_state == BatchState::ShouldFlush {
            self.flush().await?;
            self.batch.add(
                buf,
                latest_segment_count,
                latest_segment_size,
                last_total_entries,
            )?;
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), String> {
        self.prune_to_disk()
            .await
            .map_err(|e| format!("Storage (flush): {}", e))?;
        self.batch.reset();
        Ok(())
    }

    async fn prune_to_disk(&mut self) -> io::Result<usize> {
        let prune = &self.batch.get_prunable();

        let latest_partition_segment = self
            .segmentation_manager
            .get_latest_segment(DataType::Partition)
            .await?;

        let mut latest_partition_file = &latest_partition_segment.file;

        latest_partition_file
            .write_all(prune.buffer_as_bytes())
            .await?;

        for offset in prune.offsets {
            let length = self.indices.data.len();
            self.indices.data.insert(length, *offset);
        }

        let latest_indices_segment = self
            .segmentation_manager
            .get_latest_segment(DataType::Indices)
            .await?;

        let mut latest_indices_file = &latest_indices_segment.file;

        latest_indices_file
            .write_all(prune.offsets_as_bytes())
            .await?;

        Ok(prune.buffer.len())
    }

    pub fn len(&self) -> usize {
        self.indices.data.len()
    }

    pub async fn get(&mut self, index: usize) -> Option<&[u8]> {
        let offset = self.indices.data.get(&index).cloned()?;

        let segment = self
            .segmentation_manager
            .get_segment_by_index(DataType::Partition, offset.segment_count())?;

        self.seek_bytes_between(offset.start(), offset.data_size(), segment)
            .await
    }

    async fn seek_bytes_between(
        &mut self,
        start: usize,
        data_size: usize,
        segment: Arc<Segment>,
    ) -> Option<&[u8]> {
        let mut segment_file = &(*segment).file;

        if let Err(e) = segment_file.seek(SeekFrom::Start(start as u64)).await {
            println!("error {}", e);
        }

        if let Err(e) = segment_file
            .read(&mut self.retrivable_buffer[..data_size])
            .await
        {
            println!("error {}", e);
        }

        Some(&self.retrivable_buffer[..data_size])
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::macros::function;

    use super::*;

    async fn cleanup(storage: &Storage) {
        storage.directory.delete_all().await.unwrap();
    }

    async fn setup_test_storage(title: &str, test_message: &[u8], count: usize) -> Storage {
        let mut storage = Storage::new(title, false).await.unwrap();

        let messages = vec![test_message; count];

        let now = Instant::now();

        for message in messages {
            storage.set(message).await.unwrap();
        }

        let elapsed = now.elapsed();

        println!("Write {} messages in: {:.2?}", count, elapsed);

        // Make sure all messages are written to the disk before we continue with our tests
        storage.flush().await.unwrap();

        assert_eq!(storage.len(), count);

        return storage;
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn new_creates_instances() {
        // (l)eader/(r)eplica_topic-name_partition-count
        let storage = Storage::new("TEST_l_reservations_1", false).await;

        assert!(storage.is_ok());
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn get_returns_ok() {
        let message_count = 500;
        let test_message = b"messssagee";

        let mut storage = setup_test_storage(&function!(), test_message, message_count).await;

        let length = storage.len();

        let now = Instant::now();

        for index in 0..length {
            let message = storage.get(index).await;

            assert_eq!(message, Some(&test_message[..]));
        }

        let elapsed = now.elapsed();

        println!("Read {} messages in: {:.2?}", length, elapsed);

        assert_eq!(storage.len(), message_count);

        cleanup(&storage).await;
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn get_returns_none_on_index_out_of_bounds() {
        let total_count = 5;

        let test_message = b"hello world hello world hello worldrld hello worldrld hello worl";

        let mut storage = setup_test_storage(&function!(), test_message, total_count).await;

        let get_result = storage.get(total_count).await;

        assert_eq!(get_result, None);

        cleanup(&storage).await;
    }
}
