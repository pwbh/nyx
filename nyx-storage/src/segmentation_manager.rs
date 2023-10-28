use std::sync::Arc;

use async_std::{io, sync::Mutex};

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
    pub async fn new(directory: &Directory) -> io::Result<Arc<Mutex<Self>>> {
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

        Ok(Arc::new(Mutex::new(Self {
            indices_segments,
            partition_segments,
            directory: directory.clone(),
        })))
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

    fn get_last_segment_count(&self, data_type: DataType) -> usize {
        if data_type == DataType::Indices {
            self.indices_segments.len() - 1
        } else {
            self.partition_segments.len() - 1
        }
    }

    fn get_last_segment(&self, data_type: DataType) -> Option<Arc<Segment>> {
        let segment = if data_type == DataType::Indices {
            &self.indices_segments
        } else {
            &self.partition_segments
        };

        segment.last().map(|segment| segment.clone())
    }

    pub async fn get_latest_segment(
        &mut self,
        data_type: DataType,
    ) -> io::Result<(usize, Arc<Segment>)> {
        let segment_count = self.get_last_segment_count(data_type);
        // This is safe we should always have a valid segment otherwise best is crashing.
        let latest_segment = self.get_last_segment(data_type).unwrap();

        let latest_segment = if latest_segment.data.metadata().await?.len() >= MAX_SEGMENT_SIZE {
            self.create_segment(data_type).await?
        } else {
            latest_segment
        };

        Ok((segment_count, latest_segment))
    }
}
