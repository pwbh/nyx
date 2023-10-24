use core::panic;
use std::{
    collections::{hash_map::Entry, HashMap},
    io::Error,
    sync::Arc,
};

use async_std::{
    io::{self, prelude::SeekExt, ReadExt, WriteExt},
    sync::Mutex,
};

use crate::{directory::Directory, offsets::Offsets};

#[derive(Debug)]
pub struct Indices {
    pub data: HashMap<usize, Offsets>,
    pub length: usize,
    pub total_bytes: usize,
}

const INDEX_SIZE: usize = 24;

impl Indices {
    pub async fn from(directory: &Directory) -> io::Result<Arc<Mutex<Self>>> {
        let mut indices = Self {
            data: HashMap::new(),
            length: 0,
            total_bytes: 0,
        };

        let mut file = match directory
            .open_read(&crate::directory::DataType::Indices)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                println!("Warning in for indexs file: {}", e);
                return Ok(Arc::new(Mutex::new(indices)));
            }
        };

        let mut buf = [0u8; INDEX_SIZE];

        loop {
            let n = file.read(&mut buf).await?;

            if n == 0 {
                break;
            }

            file.seek(io::SeekFrom::Current(INDEX_SIZE as i64));

            let index = usize::from_le_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]);

            let start = usize::from_le_bytes([
                buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
            ]);

            let end = usize::from_le_bytes([
                buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
            ]);

            match indices.data.entry(index) {
                Entry::Occupied(_) => {
                    panic!("Something wen't wrong - index {} is already taken. Please open an issue on our Github about this.", index)
                }
                Entry::Vacant(entry) => {
                    let offsets = Offsets::new(start, end)
                        .map_err(|e| Error::new(io::ErrorKind::InvalidData, e))?;
                    entry.insert(offsets);
                }
            }
        }

        Ok(Arc::new(Mutex::new(indices)))
    }
}

#[cfg(test)]
mod tests {

    use std::io::SeekFrom;

    use crate::macros::function;

    use super::*;

    async fn create_test_data(directory: &Directory) {
        let offset = Offsets::new(15, 2500).unwrap();
        let offsets = [offset; 50];

        let mut file = directory
            .open_write(&crate::directory::DataType::Indices)
            .await
            .unwrap();

        for (index, offset) in offsets.iter().enumerate() {
            let offset_bytes = offset.as_bytes();
            let index_bytes = unsafe { *(&index as *const _ as *const [u8; 8]) };

            file.write(&index_bytes).await.unwrap();
            file.seek(SeekFrom::End(0)).await.unwrap();

            file.write(offset_bytes).await.unwrap();
            file.seek(SeekFrom::End(0)).await.unwrap();
        }
    }

    #[async_std::test]
    async fn indices_from() {
        let path = format!("./testtttt_{}", function!());
        let directory = Directory::new(&path).await.unwrap();

        create_test_data(&directory).await;

        let indices_result = Indices::from(&directory).await;

        assert!(indices_result.is_ok());

        directory
            .delete(&crate::directory::DataType::Indices)
            .await
            .unwrap();
    }
}
