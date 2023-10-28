use std::{
    io::{self, Error},
    sync::Arc,
};

use async_std::{channel::Receiver, io::WriteExt, sync::Mutex};

use crate::{
    directory::DataType, offset::Offset, segment::Segment,
    segmentation_manager::SegmentationManager, Indices, MAX_BUFFER_SIZE, MAX_SEGMENT_SIZE,
};

#[derive(Debug)]
pub struct WriteQueue {
    indices: Arc<Mutex<Indices>>,
    segmentation_manager: Arc<Mutex<SegmentationManager>>,
}

impl WriteQueue {
    pub async fn run(
        indices: Arc<Mutex<Indices>>,
        segmentation_manager: Arc<Mutex<SegmentationManager>>,
        queue: Receiver<Vec<u8>>,
    ) -> io::Result<()> {
        let mut write_queue = Self::new(indices, segmentation_manager);

        while let Ok(data) = queue.recv().await {
            write_queue.append(&data[..]).await?;
        }

        Ok(())
    }

    fn new(
        indices: Arc<Mutex<Indices>>,
        segmentation_manager: Arc<Mutex<SegmentationManager>>,
    ) -> Self {
        Self {
            indices,
            segmentation_manager,
        }
    }

    async fn get_latest_segment(
        &mut self,
        data_type: DataType,
    ) -> io::Result<(usize, Arc<Segment>)> {
        let mut segmentation_manager = self.segmentation_manager.lock().await;

        let segment_count = segmentation_manager.get_last_segment_count(data_type);
        // This is safe we should always have a valid segment otherwise best is crashing.
        let latest_segment = segmentation_manager.get_last_segment(data_type).unwrap();

        let latest_segment = if latest_segment.data.metadata().await?.len() >= MAX_SEGMENT_SIZE {
            segmentation_manager.create_segment(data_type).await?
        } else {
            latest_segment
        };

        Ok((segment_count, latest_segment))
    }

    async fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > MAX_BUFFER_SIZE {
            return Err(Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "buf size: {} kb maximum buffer size {} kb",
                    buf.len(),
                    MAX_BUFFER_SIZE
                ),
            ));
        }

        let (latest_segment_count, latest_partition_segment) =
            self.get_latest_segment(DataType::Partition).await?;

        let mut latest_partition_file = &latest_partition_segment.data;

        latest_partition_file.write_all(buf).await?;

        let mut indices = self.indices.lock().await;

        let length = indices.length;
        let total_bytes = indices.total_bytes;

        let offset = Offset::new(total_bytes, total_bytes + buf.len(), latest_segment_count)
            .map_err(|e: String| Error::new(io::ErrorKind::InvalidData, e))?;

        indices.data.insert(length, offset);
        indices.length += 1;
        indices.total_bytes += buf.len();

        drop(indices);

        let index_bytes = unsafe { *(&length as *const _ as *const [u8; 8]) };
        let offset = offset.as_bytes();

        let (_, latest_indices_segment) = self.get_latest_segment(DataType::Indices).await?;
        let mut latest_indices_file = &latest_indices_segment.data;

        latest_indices_file.write_all(&index_bytes).await?;
        latest_indices_file.write_all(offset).await?;

        Ok(buf.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::{directory::Directory, macros::function};

    use super::*;

    async fn setup_write_queue(folder: &str) -> Result<WriteQueue, Error> {
        let directory = Directory::new(&folder).await?;

        let indices = Indices::from(&directory).await?;
        let segmentation_manager = SegmentationManager::new(&directory).await?;

        Ok(WriteQueue::new(indices, segmentation_manager))
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn write_queue_instance_new_ok() {
        let folder = function!();

        let write_queue_result = setup_write_queue(&folder).await;

        assert!(write_queue_result.is_ok());
    }

    // In debug throughput was around 245 mb/s on SSD
    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn append() {
        let folder = function!();

        let write_queue_result = setup_write_queue(&folder).await;

        assert!(write_queue_result.is_ok());

        let mut write_queue = write_queue_result.unwrap();

        let count = 1_000;

        let appendables = vec![b"[\n  {\n    \"_id\": \"6530d96b2a4813b00eefdebb\",\n    \"index\": 0,\n    \"guid\": \"af98b928-0010-40b9-8383-c2e166a26730\",\n    \"isActive\": true,\n    \"balance\": \"$1,236.95\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 35,\n    \"eyeColor\": \"brown\",\n    \"name\": \"Combs Donovan\",\n    \"gender\": \"male\",\n    \"company\": \"CEDWARD\",\n    \"email\": \"combsdonovan@cedward.com\",\n    \"phone\": \"+1 (851) 521-3998\",\n    \"address\": \"614 Bristol Street, Sperryville, North Dakota, 1714\",\n    \"about\": \"Pariatur ex sit qui est dolor id laboris dolor proident ipsum ea. Est nisi deserunt tempor est Lorem enim. Ex proident proident cupidatat ullamco voluptate non pariatur voluptate eiusmod. Eu voluptate do quis nulla nisi sint elit dolor proident culpa.\\r\\n\",\n    \"registered\": \"2020-09-29T07:14:44 -03:00\",\n    \"latitude\": -49.760699,\n    \"longitude\": -120.871902,\n    \"tags\": [\n      \"eiusmod\",\n      \"anim\",\n      \"excepteur\",\n      \"qui\",\n      \"dolore\",\n      \"laborum\",\n      \"elit\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Allison Stanley\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Stewart Campbell\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Reese Hensley\"\n      }\n    ],\n    \"greeting\": \"Hello, Combs Donovan! You have 5 unread messages.\",\n    \"favoriteFruit\": \"banana\"\n  },\n  {\n    \"_id\": \"6530d96bdd1e27f2847a41b7\",\n    \"index\": 1,\n    \"guid\": \"c353117f-3e97-4052-8189-53971370bea0\",\n    \"isActive\": false,\n    \"balance\": \"$3,921.66\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 36,\n    \"eyeColor\": \"brown\",\n    \"name\": \"Socorro Delacruz\",\n    \"gender\": \"female\",\n    \"company\": \"EYERIS\",\n    \"email\": \"socorrodelacruz@eyeris.com\",\n    \"phone\": \"+1 (812) 464-2296\",\n    \"address\": \"849 Bank Street, Madaket, New Mexico, 5302\",\n    \"about\": \"Nisi aute duis minim tempor cupidatat elit quis nisi adipisicing esse ipsum pariatur sunt. Incididunt velit velit ullamco occaecat non pariatur consectetur. Occaecat magna tempor id et consequat sit ipsum voluptate sunt non velit fugiat Lorem ex. Deserunt aute ut exercitation laborum aute reprehenderit veniam consequat laboris fugiat officia aute sunt quis. Elit voluptate ad veniam laboris nisi aliquip dolor enim aute mollit tempor. Duis ex aliquip sit culpa commodo et culpa. Lorem in reprehenderit consectetur adipisicing aute id.\\r\\n\",\n    \"registered\": \"2018-06-08T09:30:58 -03:00\",\n    \"latitude\": -63.990273,\n    \"longitude\": -177.365978,\n    \"tags\": [\n      \"non\",\n      \"commodo\",\n      \"sint\",\n      \"nisi\",\n      \"laboris\",\n      \"in\",\n      \"fugiat\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Lauren Livingston\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Erna Maynard\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Crystal Church\"\n      }\n    ],\n    \"greeting\": \"Hello, Socorro Delacruz! You have 3 unread messages.\",\n    \"favoriteFruit\": \"apple\"\n  },\n  {\n    \"_id\": \"6530d96b4fa4b866b370aa63\",\n    \"index\": 2,\n    \"guid\": \"af05e6e8-06e9-43a7-91fa-d51bd470d4d3\",\n    \"isActive\": false,\n    \"balance\": \"$2,721.27\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 39,\n    \"eyeColor\": \"green\",\n    \"name\": \"Debbie Benson\",\n    \"gender\": \"female\",\n    \"company\": \"ACIUM\",\n    \"email\": \"debbiebenson@acium.com\",\n    \"phone\": \"+1 (924) 579-2622\",\n    \"address\": \"598 King Street, Edgewater, Hawaii, 843\",\n    \"about\": \"Sit dolor ullamco elit velit do. Ex cillum culpa aliqua ut fugiat. Veniam irure est minim amet cupidatat laborum ad elit sit culpa mollit do. Qui sit commodo nisi aute sint laborum proident ut exercitation tempor Lorem exercitation excepteur. Consequat in in deserunt minim.\\r\\n\",\n    \"registered\": \"2020-03-28T12:30:27 -03:00\",\n    \"latitude\": 32.696383,\n    \"longitude\": 120.139506,\n    \"tags\": [\n      \"officia\",\n      \"amet\",\n      \"esse\",\n      \"consequat\",\n      \"duis\",\n      \"nulla\",\n      \"aute\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Long Mccormick\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Alana Horne\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Kathleen Haynes\"\n      }\n    ],\n    \"greeting\": \"Hello, Debbie Benson! You have 1 unread messages.\",\n    \"favoriteFruit\": \"banana\"\n  },\n  {\n    \"_id\": \"6530d96bfb424edf488996bc\",\n    \"index\": 3,\n    \"guid\": \"0b749df5-bb2e-45e1-8f9e-e56c2ebded50\",\n    \"isActive\": false,\n    \"balance\": \"$3,900.53\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 24,\n    \"eyeColor\": \"blue\",\n    \"name\": \"Cervantes Stone\",\n    \"gender\": \"male\",\n    \"company\": \"LOCAZONE\",\n    \"email\": \"cervantesstone@locazone.com\",\n    \"phone\": \"+1 (913) 517-3407\",\n    \"address\": \"168 Hutchinson Court, Calpine, South Carolina, 1338\",\n    \"about\": \"Qui duis excepteur do consectetur dolore id in qui aliquip elit laborum dolor nulla nulla. Mollit magna ut fugiat magna id est non laborum aute non incididunt. Et exercitation est est deserunt do. Eiusmod adipisicing consectetur ullamco veniam exercitation cillum ad velit est. Velit exercitation Lorem aute incididunt irure officia pariatur duis reprehenderit ad commodo. Labore velit in duis labore pariatur laboris duis commodo velit aliqua commodo.\\r\\n\",\n    \"registered\": \"2017-07-16T01:33:27 -03:00\",\n    \"latitude\": 63.81319,\n    \"longitude\": 143.20968,\n    \"tags\": [\n      \"velit\",\n      \"labore\",\n      \"in\",\n      \"duis\",\n      \"amet\",\n      \"pariatur\",\n      \"occaecat\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Muriel Cobb\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Bartlett Blevins\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Alison Parrish\"\n      }\n    ],\n    \"greeting\": \"Hello, Cervantes Stone! You have 2 unread messages.\",\n    \"favoriteFruit\": \"strawberry\"\n  },\n  {\n    \"_id\": \"6530d96b6abc0fd6205b2931\",\n    \"index\": 4,\n    \"guid\": \"7af01846-5852-47cf-809a-9d7cff0a5f33\",\n    \"isActive\": false,\n    \"balance\": \"$2,612.30\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 27,\n    \"eyeColor\": \"brown\",\n    \"name\": \"Elsie Hobbs\",\n    \"gender\": \"female\",\n    \"company\": \"BOINK\",\n    \"email\": \"elsiehobbs@boink.com\",\n    \"phone\": \"+1 (983) 589-3960\",\n    \"address\": \"102 Cropsey Avenue, Frierson, Kansas, 1379\",\n    \"about\": \"Voluptate fugiat cillum eiusmod Lorem elit tempor adipisicing aute. Elit sunt laborum deserunt est ipsum sunt aute enim ex ullamco. Ullamco veniam nulla ex id anim ullamco labore. Nisi in Lorem laborum in duis excepteur voluptate sint culpa velit laborum incididunt.\\r\\n\",\n    \"registered\": \"2022-06-12T06:02:07 -03:00\",\n    \"latitude\": -83.164798,\n    \"longitude\": 144.735765,\n    \"tags\": [\n      \"id\",\n      \"nostrud\",\n      \"do\",\n      \"quis\",\n      \"consequat\",\n      \"dolore\",\n      \"irure\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Jacqueline Salazar\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Callahan Chavez\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Hull Callahan\"\n      }\n    ],\n    \"greeting\": \"Hello, Elsie Hobbs! You have 6 unread messages.\",\n    \"favoriteFruit\": \"banana\"\n  },\n  {\n    \"_id\": \"6530d96b6ccdb5a05ac42d89\",\n    \"index\": 5,\n    \"guid\": \"9270e977-09b1-49ff-a089-3daf6b0f6638\",\n    \"isActive\": false,\n    \"balance\": \"$1,178.38\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 32,\n    \"eyeColor\": \"green\",\n    \"name\": \"Flossie Higgins\",\n    \"gender\": \"female\",\n    \"company\": \"ASSISTIX\",\n    \"email\": \"flossiehiggins@assistix.com\",\n    \"phone\": \"+1 (920) 559-2423\",\n    \"address\": \"373 Navy Street, Kohatk, South Dakota, 9646\",\n    \"about\": \"Enim nulla culpa nostrud dolor quis et culpa consequat exercitation in deserunt. Officia voluptate nulla laborum sunt et incididunt ut sint quis. Eiusmod enim tempor irure amet aute ipsum culpa esse id cupidatat ea. Occaecat laboris proident consequat ullamco. Ea est nostrud aliqua eiusmod amet quis. Non dolor pariatur dolor incididunt aliquip occaecat adipisicing fugiat pariatur Lorem duis enim voluptate elit.\\r\\n\",\n    \"registered\": \"2014-10-30T10:11:50 -02:00\",\n    \"latitude\": 61.493115,\n    \"longitude\": 102.592193,\n    \"tags\": [\n      \"pariatur\",\n      \"et\",\n      \"exercitation\",\n      \"cupidatat\",\n      \"sit\",\n      \"anim\",\n      \"consequat\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Daphne Velasquez\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Melanie Leon\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Young Carter\"\n      }\n    ],\n    \"greeting\": \"Hello, Flossie Higgins! You have 5 unread messages.\",\n    \"favoriteFruit\": \"strawberry\"\n  }\n]"; count];

        for appendable in appendables {
            let append_result = write_queue.append(appendable).await.unwrap();
            assert_eq!(append_result, appendable.len());
        }
    }
}
