use std::{collections::HashMap, sync::Arc};

use async_std::io::{self, prelude::SeekExt, ReadExt};

use crate::{offset::Offset, segment::Segment};

#[derive(Debug)]
pub struct Indices {
    pub data: HashMap<usize, Offset>,
    pub total_bytes: usize,
}

const OFFSET_SIZE: usize = std::mem::size_of::<Offset>();

impl Indices {
    pub async fn from(segments: &[Arc<Segment>]) -> io::Result<Self> {
        let mut indices = Self {
            data: HashMap::new(),
            total_bytes: 0,
        };

        for segment in segments {
            let mut buf: [u8; 32] = [0u8; OFFSET_SIZE];
            let mut file = &(*segment).file;

            loop {
                let n = file.read(&mut buf).await?;

                if n == 0 {
                    break;
                }

                file.seek(io::SeekFrom::Current(OFFSET_SIZE as i64));

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

                indices.total_bytes += data_size;
            }
        }

        Ok(indices)
    }
}

#[cfg(test)]
mod tests {
    use async_std::io::WriteExt;

    use crate::{directory::Directory, macros::function};

    use super::*;

    async fn create_test_data(directory: &Directory) -> Vec<Arc<Segment>> {
        let mut offsets = vec![];

        for i in 0..50 {
            let offset = Offset::new(i, 15, 2500, 0).unwrap();
            offsets.push(offset);
        }

        let mut file = directory
            .open_write(crate::directory::DataType::Indices, 0)
            .await
            .unwrap();

        for offset in offsets.iter() {
            let offset_bytes = offset.as_bytes();
            file.write_all(offset_bytes).await.unwrap();
        }

        let segment = Segment::new(&directory, crate::directory::DataType::Indices, 0)
            .await
            .unwrap();

        vec![Arc::new(segment)]
    }

    #[async_std::test]
    async fn indices_from() {
        let path = format!("./{}", function!());
        let directory = Directory::new(&path).await.unwrap();
        let segments = create_test_data(&directory).await;
        let indices_result = Indices::from(&segments).await.unwrap();

        for (k, v) in indices_result.data {
            assert_eq!(v, Offset::new(k, 15, 2500, 0).unwrap())
        }

        directory
            .delete_file(crate::directory::DataType::Indices, 0)
            .await
            .unwrap();
    }
}
