use std::io;

use async_std::{channel::Receiver, fs::OpenOptions, io::WriteExt, path::Path};

pub struct WriteQueue;

impl WriteQueue {
    pub async fn run(queue: Receiver<Vec<u8>>, path: &Path) -> io::Result<()> {
        let mut data_file = OpenOptions::new().append(true).open(path).await?;

        data_file.write_all(b"I am trying to write").await?;

        while let Ok(data) = queue.recv().await {
            println!("Do something with received data: {:?}", data);
        }

        Ok(())
    }
}
