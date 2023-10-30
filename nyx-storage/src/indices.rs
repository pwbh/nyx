use std::collections::HashMap;

use async_std::io::{self, prelude::SeekExt, ReadExt, WriteExt};

use crate::{directory::Directory, offset::Offset};

#[derive(Debug)]
pub struct Indices {
    pub data: HashMap<usize, Offset>,
    pub total_bytes: usize,
}

const INDEX_SIZE: usize = std::mem::size_of::<Offset>();

impl Indices {
    pub async fn from(directory: &Directory) -> io::Result<Self> {
        let mut indices = Self {
            data: HashMap::new(),
            total_bytes: 0,
        };

        let mut file = match directory
            .open_read(crate::directory::DataType::Indices, 0)
            .await
        {
            Ok(file) => file,
            Err(e) => {
                println!("Warning in for indexs file: {}", e);
                return Ok(indices);
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

            let data_size = usize::from_le_bytes([
                buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
            ]);

            let segment_index = usize::from_le_bytes([
                buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
            ]);

            indices
                .data
                .insert(index, Offset::from(index, start, data_size, segment_index));
        }

        Ok(indices)
    }
}

#[cfg(test)]
mod tests {

    use std::io::SeekFrom;

    use crate::macros::function;

    use super::*;

    async fn create_test_data(directory: &Directory) {
        let mut offsets = vec![];

        for i in 0..50 {
            let offset = Offset::new(i, 15, 2500, 0).unwrap();
            offsets.push(offset);
        }

        let mut file = directory
            .open_write(crate::directory::DataType::Indices, 0)
            .await
            .unwrap();

        for (index, offset) in offsets.iter().enumerate() {
            let index_bytes = unsafe { *(&index as *const _ as *const [u8; 8]) };
            let offset_bytes = offset.as_bytes();

            file.write_all(&index_bytes).await.unwrap();
            file.seek(SeekFrom::End(0)).await.unwrap();

            file.write_all(offset_bytes).await.unwrap();
            file.seek(SeekFrom::End(0)).await.unwrap();
        }
    }

    #[async_std::test]
    async fn indices_from() {
        let path = format!("./{}", function!());
        let directory = Directory::new(&path).await.unwrap();

        create_test_data(&directory).await;

        let indices_result = Indices::from(&directory).await;

        assert!(indices_result.is_ok());

        directory
            .delete_file(crate::directory::DataType::Indices, 0)
            .await
            .unwrap();
    }
}
