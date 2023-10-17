use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Seek},
    path::Path,
    sync::Arc,
};

use async_std::channel::{bounded, Sender};
use storage_sender::StorageSender;
use write_queue::WriteQueue;

mod storage_sender;
mod write_queue;

#[derive(Clone, Copy)]
struct Offsets {
    start: usize,
    end: usize,
}

impl Offsets {
    pub fn new(start: usize, end: usize) -> Result<Self, String> {
        if start >= end {
            return Err(format!(
                "Start ({}) can't be greater o equal to end ({}) of file byte",
                start, end
            ));
        }

        Ok(Self { start, end })
    }
}

pub struct Storage {
    indices: HashMap<usize, Offsets>,
    file: Arc<File>,
    // TODO: When Storage will be accessed concurrently each concurrent accessor should have a
    // `retrievable_buffer` of its own to read into instead.
    retrivable_buffer: [u8; 8192],
    write_sender: Sender<Vec<u8>>,
}

impl Storage {
    pub fn new(path: &Path, max_queue: usize) -> Result<Self, String> {
        let (write_sender, write_receiver) = bounded(max_queue);

        async_std::task::spawn(WriteQueue::run(write_receiver));

        Ok(Self {
            indices: HashMap::new(),
            file: Arc::new(File::open(path).map_err(|e| e.to_string())?),
            retrivable_buffer: [0; 8192],
            write_sender,
        })
    }

    pub fn new_sender(&mut self) -> Result<StorageSender, String> {
        Ok(StorageSender::new(self.write_sender.clone()))
    }

    pub fn get(&mut self, index: usize) -> Result<&[u8], String> {
        let offsets = self
            .indices
            .get(&index)
            .ok_or("record doesn't exist.".to_string())?;

        let data_size = offsets.end - offsets.start;

        if data_size > self.retrivable_buffer.len() {
            return Err(format!(
                "Seeked data size: {}kb maximum retrivable size {}kb",
                data_size,
                self.retrivable_buffer.len()
            ));
        }

        return self
            .seek_bytes_between(offsets.start, data_size)
            .map_err(|e| format!("Error in Storage (get): {}", e));
    }

    fn seek_bytes_between(&mut self, start: usize, data_size: usize) -> io::Result<&[u8]> {
        let mut file = &*self.file;
        file.seek(std::io::SeekFrom::Start(start as u64))?;
        file.read_exact(&mut self.retrivable_buffer[..data_size])?;
        file.rewind()?;
        return Ok(&self.retrivable_buffer[..data_size]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
