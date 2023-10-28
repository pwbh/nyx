use std::{
    io::{self, Error},
    sync::Arc,
};

use async_std::{channel::Receiver, io::WriteExt, sync::Mutex};

use crate::{
    directory::DataType, offset::Offset, segmentation_manager::SegmentationManager, Indices,
    MAX_BUFFER_SIZE,
};

#[derive(Debug)]
pub struct WriteQueue {
    indices: Arc<Mutex<Indices>>,
    segmentation_manager: Arc<Mutex<SegmentationManager>>,
}

impl WriteQueue {
    pub async fn run(
        indices: Arc<Mutex<Indices>>,
        segmentation_manager: Arc<Mutex<SegmentationManager>>,
        queue: Receiver<Vec<u8>>,
    ) -> io::Result<()> {
        let mut write_queue = Self::new(indices, segmentation_manager);

        while let Ok(data) = queue.recv().await {
            write_queue.append(&data[..]).await?;
        }

        Ok(())
    }

    fn new(
        indices: Arc<Mutex<Indices>>,
        segmentation_manager: Arc<Mutex<SegmentationManager>>,
    ) -> Self {
        Self {
            indices,
            segmentation_manager,
        }
    }

    async fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut segmentation_manager = self.segmentation_manager.lock().await;

        if buf.len() > MAX_BUFFER_SIZE {
            return Err(Error::new(
                io::ErrorKind::InvalidData,
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
            .map_err(|e: String| Error::new(io::ErrorKind::InvalidData, e))?;

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
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::{directory::Directory, macros::function};

    use super::*;

    async fn setup_write_queue(folder: &str) -> Result<WriteQueue, Error> {
        let directory = Directory::new(&folder).await?;

        let indices = Indices::from(&directory).await?;
        let segmentation_manager = SegmentationManager::new(&directory).await?;

        Ok(WriteQueue::new(indices, segmentation_manager))
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn write_queue_instance_new_ok() {
        let folder = function!();

        let write_queue_result = setup_write_queue(&folder).await;

        assert!(write_queue_result.is_ok());
    }

    // In debug throughput was around 245 mb/s on SSD
    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn append() {
        let folder = function!();

        let write_queue_result = setup_write_queue(&folder).await;

        assert!(write_queue_result.is_ok());

        let mut write_queue = write_queue_result.unwrap();

        let count = 1_000;

        let appendables = vec![b"{\n  \"user_id\": 12345,\n  \"username\": \"john_doe\",\n  \"full_name\": \"John Doe\",\n  \"email\": \"john.doe@example.com\",\n  \"age\": 30,\n  \"is_active\": true,\n  \"registration_date\": \"2023-01-15\",\n  \"address\": {\n    \"street\": \"123 Main St\",\n    \"city\": \"Anytown\",\n    \"state\": \"CA\",\n    \"postal_code\": \"12345\"\n  },\n  \"interests\": [\"Hiking\", \"Photography\", \"Cooking\"],\n  \"friends\": [\n    {\n      \"friend_id\": 6789,\n      \"friend_username\": \"jane_smith\"\n    },\n    {\n      \"friend_id\": 9876,\n      \"friend_username\": \"mike_jones\"\n    }\n  ],\n  \"preferences\": {\n    \"theme\": \"light\",\n    \"notifications\": {\n      \"email\": true,\n      \"push\": true\n    }\n  },\n  \"additional_data\": \"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam bibendum tristique justo, in varius quam interdum et. Sed congue velit id justo consectetur, eget egestas libero elementum. Phasellus convallis erat ut lorem pellentesque, eget rhoncus tellus vestibulum. Vestibulum sit amet leo eu lectus rhoncus blandit. Curabitur vel convallis eros. Sed bibendum ipsum quis mauris iaculis, id efficitur augue rhoncus. Maecenas ultricies est et quam laoreet, ac tincidunt metus condimentum. Nam at varius est, at bibendum sem. Sed in consectetur odio. Vivamus id sapien vel elit euismod fermentum. Duis convallis massa nec urna dignissim, nec facilisis orci vestibulum. Cras eget velit vitae nulla tempor rhoncus. Vivamus eu metus a libero posuere cursus. Sed ultrices, augue eu facilisis bibendum, justo odio euismod massa, a tincidunt ex purus quis nunc. Morbi eget luctus libero.\",\n  \"more_data\": \"Proin non eros nec urna posuere hendrerit. Quisque scelerisque risus vel turpis hendrerit, at vehicula urna feugiat. Vestibulum vitae varius nulla. Praesent posuere sit amet sem eu cursus. Nam pulvinar, justo ac condimentum lacinia, sem odio laoreet odio, ac tincidunt ex libero nec enim. Nunc ut purus eu elit auctor dictum. Integer tristique bibendum nulla, in euismod velit dapibus ut. Sed eu bibendum odio. In hac habitasse platea dictumst. Fusce ac mauris ut massa hendrerit efficitur.\",\n  \"last_data\": \"Aenean eu vestibulum purus. Curabitur in dui eget libero tincidunt euismod. Suspendisse vel elit non quam hendrerit bibendum. Sed auctor, nunc vel semper malesuada, quam odio bibendum eros, sit amet euismod risus purus ac lectus. Nullam pharetra risus eget ante bibendum, at lacinia justo posuere. Sed in nunc non libero laoreet suscipit. Etiam auctor, elit quis dignissim sollicitudin, massa eros facilisis ante, non ullamcorper metus elit at odio. Donec non felis nec lorem efficitur dictum vel non nulla.\"\n}\n"; count];

        let now = Instant::now();

        for appendable in appendables {
            let append_result = write_queue.append(appendable).await.unwrap();
            assert_eq!(append_result, appendable.len());
        }

        let elapsed = now.elapsed();

        println!("Appended in: {:.2?}", elapsed);
    }
}
