use std::{ptr::NonNull, sync::Arc};

use async_std::io;

use crate::{
    directory::{DataType, Directory},
    segment::Segment,
    MAX_SEGMENT_SIZE,
};

#[derive(PartialEq)]
pub enum SegmentMode {
    Write,
    Read,
}

#[derive(Debug)]
pub struct SegmentationManager {
    indices_segments: Vec<Arc<Segment>>,
    pub partition_segments: Vec<Arc<Segment>>,
    latest_index_segment: NonNull<Vec<Arc<Segment>>>,
    directory: Directory,
}

impl SegmentationManager {
    pub async fn new(directory: &Directory) -> io::Result<Self> {
        let latest_indices_segment =
            Segment::new(&directory, crate::directory::DataType::Indices, 0).await?;

        let latest_partition_segment =
            Segment::new(&directory, crate::directory::DataType::Partition, 0).await?;

        Ok(Self {
            indices_segments: vec![Arc::new(latest_indices_segment)],
            partition_segments: vec![Arc::new(latest_partition_segment)],
            directory: directory.clone(),
            latest_index_segment: NonNull::dangling(),
        })
    }

    pub async fn from(directory: &Directory) -> io::Result<Self> {
        let mut indices_segments = vec![];
        let mut partition_segments = vec![];

        let mut current_segment_candidate = 0;

        while let Some(file) = directory
            .open_read_write(DataType::Partition, current_segment_candidate)
            .await
            .ok()
        {
            current_segment_candidate += 1;
            partition_segments.push(Arc::new(Segment::from(DataType::Partition, file).await?));
        }

        current_segment_candidate = 0;

        while let Some(file) = directory
            .open_read_write(DataType::Indices, current_segment_candidate)
            .await
            .ok()
        {
            current_segment_candidate += 1;
            indices_segments.push(Arc::new(Segment::from(DataType::Indices, file).await?));
        }

        if indices_segments.len() == 0 {
            let latest_indices_segment =
                Segment::new(&directory, crate::directory::DataType::Indices, 0).await?;
            indices_segments.push(Arc::new(latest_indices_segment));
        }

        if partition_segments.len() == 0 {
            let latest_partition_segment =
                Segment::new(&directory, crate::directory::DataType::Partition, 0).await?;
            partition_segments.push(Arc::new(latest_partition_segment));
        }

        Ok(Self {
            indices_segments,
            partition_segments,
            latest_index_segment: NonNull::dangling(),
            directory: directory.clone(),
        })
    }

    pub fn partition_segments(&self) -> &[Arc<Segment>] {
        &self.partition_segments[..]
    }

    pub fn indices_segments(&self) -> &[Arc<Segment>] {
        &self.indices_segments[..]
    }

    pub async fn create_segment(&mut self, data_type: DataType) -> io::Result<Arc<Segment>> {
        let new_segment_count = if data_type == DataType::Indices {
            self.indices_segments.len()
        } else {
            self.partition_segments.len()
        };

        let new_segment = Segment::new(&self.directory, data_type, new_segment_count).await?;

        let new_segment = Arc::new(new_segment);

        if data_type == DataType::Indices {
            self.indices_segments.push(new_segment.clone());
        } else {
            self.partition_segments.push(new_segment.clone());
        }

        Ok(new_segment)
    }

    pub fn get_segment_by_index(&self, data_type: DataType, index: usize) -> Option<Arc<Segment>> {
        let segments = if data_type == DataType::Indices {
            &self.indices_segments
        } else {
            &self.partition_segments
        };

        segments.get(index).map(|segment| segment.clone())
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
        .file
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
        // This is safe we should always have a valid segment otherwise best is to crash ASAP.
        let latest_segment = self.get_last_segment(data_type).unwrap();

        if latest_segment.file.metadata().await?.len() >= MAX_SEGMENT_SIZE {
            self.create_segment(data_type).await
        } else {
            Ok(latest_segment)
        }
    }
}
