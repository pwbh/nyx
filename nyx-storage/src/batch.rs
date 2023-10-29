use crate::{offset::Offset, MAX_BATCH_SIZE};

#[derive(PartialEq)]
pub enum BatchState {
    ShouldPrune,
    Allowable,
}

pub struct Prune<'a> {
    pub buffer: &'a [u8],
    pub offsets: &'a [Offset],
}

#[derive(Debug)]
pub struct Batch {
    buffer: [u8; MAX_BATCH_SIZE],
    offsets: Vec<Offset>,
    current_batch_size: usize,
}

impl Batch {
    pub fn new(current_segment_size: usize) -> Self {
        Self {
            buffer: [0; MAX_BATCH_SIZE],
            offsets: vec![],
            current_batch_size: 0,
        }
    }

    pub fn add(
        &mut self,
        buf: &[u8],
        latest_segment_count: usize,
        latest_segment_size: usize,
    ) -> Result<BatchState, String> {
        if self.current_batch_size + buf.len() <= self.buffer.len() {
            let offset = Offset::new(
                latest_segment_size,
                latest_segment_size + buf.len(),
                latest_segment_count,
            )?;

            self.buffer[self.current_batch_size..self.current_batch_size + buf.len()]
                .copy_from_slice(buf);
            self.current_batch_size += buf.len();
            self.offsets.push(offset);

            Ok(BatchState::Allowable)
        } else {
            Ok(BatchState::ShouldPrune)
        }
    }

    pub fn reset(&mut self) {
        self.offsets.clear();
        self.current_batch_size = 0;
    }

    pub fn get_prunable(&self) -> Prune<'_> {
        Prune {
            buffer: &self.buffer[..self.current_batch_size],
            offsets: &self.offsets[..],
        }
    }
}
