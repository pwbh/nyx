use std::sync::Arc;

use async_std::io;

use crate::{
    directory::{DataType, Directory},
    segment::Segment,
    MAX_SEGMENT_SIZE,
};

#[derive(Debug)]
pub struct SegmentationManager {
    indices_segments: Vec<Arc<Segment>>,
    partition_segments: Vec<Arc<Segment>>,
    directory: Directory,
}

impl SegmentationManager {
    pub async fn new(directory: &Directory) -> io::Result<Self> {
        let latest_indices_segment = Segment::new(
            &directory,
            crate::directory::DataType::Indices,
            MAX_SEGMENT_SIZE,
            0,
        )
        .await?;

        let latest_partition_segment = Segment::new(
            &directory,
            crate::directory::DataType::Partition,
            MAX_SEGMENT_SIZE,
            0,
        )
        .await?;

        let indices_segments = vec![Arc::new(latest_indices_segment)];
        let partition_segments = vec![Arc::new(latest_partition_segment)];

        Ok(Self {
            indices_segments,
            partition_segments,
            directory: directory.clone(),
        })
    }

    pub async fn create_segment(&mut self, data_type: DataType) -> io::Result<Arc<Segment>> {
        let new_segment_count = if data_type == DataType::Indices {
            self.indices_segments.len()
        } else {
            self.partition_segments.len()
        };

        let new_segment = Segment::new(
            &self.directory,
            data_type,
            MAX_SEGMENT_SIZE,
            new_segment_count,
        )
        .await?;

        let new_segment = Arc::new(new_segment);

        if data_type == DataType::Indices {
            self.indices_segments.push(new_segment.clone());
        } else {
            self.partition_segments.push(new_segment.clone());
        }

        Ok(new_segment)
    }

    pub fn get_last_segment_count(&self, data_type: DataType) -> usize {
        if data_type == DataType::Indices {
            self.indices_segments.len() - 1
        } else {
            self.partition_segments.len() - 1
        }
    }

    pub async fn get_last_segment_size(&self, data_type: DataType) -> usize {
        // These unwraps are safe
        if data_type == DataType::Indices {
            self.indices_segments.last()
        } else {
            self.partition_segments.last()
        }
        .unwrap()
        .data
        .metadata()
        .await
        .unwrap()
        .len() as usize
    }

    fn get_last_segment(&self, data_type: DataType) -> Option<Arc<Segment>> {
        let segment = if data_type == DataType::Indices {
            &self.indices_segments
        } else {
            &self.partition_segments
        };

        segment.last().map(|segment| segment.clone())
    }

    pub async fn get_latest_segment(&mut self, data_type: DataType) -> io::Result<Arc<Segment>> {
        // This is safe we should always have a valid segment otherwise best is crashing.
        let latest_segment = self.get_last_segment(data_type).unwrap();

        if latest_segment.data.metadata().await?.len() >= MAX_SEGMENT_SIZE {
            self.create_segment(data_type).await
        } else {
            Ok(latest_segment)
        }
    }
}
