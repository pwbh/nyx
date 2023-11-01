use crate::{offset::Offset, MAX_BATCH_SIZE};

#[derive(PartialEq)]
pub enum BatchState {
    ShouldFlush,
    Allowable,
}

#[derive(Debug)]
pub struct Prune<'a> {
    pub buffer: &'a [u8],
    pub offsets: &'a [Offset],
}

impl<'a> Prune<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.buffer.as_ptr() as *const u8,
                self.buffer.len() * std::mem::size_of::<Offset>(),
            )
        }
    }
}

#[derive(Debug)]
pub struct Batch {
    buffer: [u8; MAX_BATCH_SIZE],
    offsets: Vec<Offset>,
    current_batch_size: usize,
    current_batch_index: usize,
    current_segment_size: usize,
}

impl Batch {
    pub fn new() -> Self {
        Self {
            buffer: [0; MAX_BATCH_SIZE],
            offsets: vec![],
            current_batch_size: 0,
            current_batch_index: 0,
            current_segment_size: 0,
        }
    }

    pub fn add(
        &mut self,
        buf: &[u8],
        latest_segment_count: usize,
        latest_segment_size: usize,
    ) -> Result<BatchState, String> {
        if self.current_batch_size + buf.len() < MAX_BATCH_SIZE {
            if self.current_batch_index == 0 {
                self.current_batch_index = latest_segment_count;
                self.current_segment_size = latest_segment_size;
            }

            let offset = Offset::new(
                self.current_batch_index,
                self.current_segment_size,
                self.current_segment_size + buf.len(),
                latest_segment_count,
            )?;

            self.buffer[self.current_batch_size..self.current_batch_size + buf.len()]
                .copy_from_slice(buf);
            self.current_batch_size += buf.len();
            self.current_segment_size += buf.len();
            self.offsets.push(offset);
            self.current_batch_index += 1;

            Ok(BatchState::Allowable)
        } else {
            Ok(BatchState::ShouldFlush)
        }
    }

    pub fn reset(&mut self) {
        self.offsets.clear();
        self.current_batch_size = 0;
        self.current_batch_index = 0;
    }

    pub fn get_prunable(&self) -> Prune<'_> {
        Prune {
            buffer: &self.buffer[..self.current_batch_size],
            offsets: &self.offsets[..],
        }
    }
}
