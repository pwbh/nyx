use std::{
    io::{self, SeekFrom},
    sync::Arc,
};

use async_std::{
    channel::Receiver,
    fs::File,
    io::{prelude::SeekExt, WriteExt},
};

pub struct WriteQueue {
    file: Arc<File>,
    current_last_byte: usize,
}

impl WriteQueue {
    pub async fn new(file: Arc<File>) -> io::Result<Self> {
        let mut file_ref = &*file;
        file_ref.seek(SeekFrom::End(0)).await?;

        Ok(Self {
            file,
            current_last_byte: 0,
        })
    }

    async fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut file = &*self.file;
        let n = file.write(buf).await?;
        self.current_last_byte += n;
        file.seek(SeekFrom::End(0)).await?;
        Ok(())
    }

    pub async fn run(queue: Receiver<Vec<u8>>, file: Arc<File>) -> io::Result<()> {
        let mut write_queue = Self::new(file).await?;

        while let Ok(data) = queue.recv().await {
            write_queue.write(&data[..]).await?;
        }

        Ok(())
    }

    // pub
}
