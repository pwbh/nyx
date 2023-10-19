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
}

impl WriteQueue {
    pub async fn new(file: Arc<File>) -> io::Result<Self> {
        let mut file_ref = &*file;
        file_ref.seek(SeekFrom::End(0)).await?;

        Ok(Self { file })
    }

    async fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        let mut file = &*self.file;
        file.write(buf).await?;
        file.seek(SeekFrom::End(0)).await?;
        Ok(())
    }

    pub async fn run(queue: Receiver<Vec<u8>>, file: Arc<File>) -> io::Result<()> {
        let mut write_queue = Self::new(file).await?;

        while let Ok(data) = queue.recv().await {
            write_queue.append(&data[..]).await?;
        }

        Ok(())
    }

    // pub
}

#[cfg(test)]
mod tests {
    use async_std::fs;

    use crate::macros::function;

    use super::*;

    async fn create_test_file(test_file_path: &str) -> File {
        File::create(test_file_path).await.unwrap()
    }

    async fn cleanup_test_file(test_file_path: &str) -> io::Result<()> {
        fs::remove_file(test_file_path).await
    }

    #[async_std::test]
    async fn write_queue_instance_new_ok() {
        let test_file_path = format!("./{}.data", function!());

        let file = Arc::new(create_test_file(&test_file_path).await);
        let write_queue_result = WriteQueue::new(file.clone()).await;

        assert!(write_queue_result.is_ok());

        let cleanup_result = cleanup_test_file(&test_file_path).await;

        assert!(cleanup_result.is_ok());
    }

    #[async_std::test]
    async fn append() {
        let test_file_path = format!("./{}.data", function!());

        let file = Arc::new(create_test_file(&test_file_path).await);
        let write_queue_result = WriteQueue::new(file.clone()).await;

        assert!(write_queue_result.is_ok());

        let mut write_queue = write_queue_result.unwrap();

        let appendables = vec![b"[\n  {\n    \"_id\": \"6530d96b2a4813b00eefdebb\",\n    \"index\": 0,\n    \"guid\": \"af98b928-0010-40b9-8383-c2e166a26730\",\n    \"isActive\": true,\n    \"balance\": \"$1,236.95\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 35,\n    \"eyeColor\": \"brown\",\n    \"name\": \"Combs Donovan\",\n    \"gender\": \"male\",\n    \"company\": \"CEDWARD\",\n    \"email\": \"combsdonovan@cedward.com\",\n    \"phone\": \"+1 (851) 521-3998\",\n    \"address\": \"614 Bristol Street, Sperryville, North Dakota, 1714\",\n    \"about\": \"Pariatur ex sit qui est dolor id laboris dolor proident ipsum ea. Est nisi deserunt tempor est Lorem enim. Ex proident proident cupidatat ullamco voluptate non pariatur voluptate eiusmod. Eu voluptate do quis nulla nisi sint elit dolor proident culpa.\\r\\n\",\n    \"registered\": \"2020-09-29T07:14:44 -03:00\",\n    \"latitude\": -49.760699,\n    \"longitude\": -120.871902,\n    \"tags\": [\n      \"eiusmod\",\n      \"anim\",\n      \"excepteur\",\n      \"qui\",\n      \"dolore\",\n      \"laborum\",\n      \"elit\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Allison Stanley\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Stewart Campbell\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Reese Hensley\"\n      }\n    ],\n    \"greeting\": \"Hello, Combs Donovan! You have 5 unread messages.\",\n    \"favoriteFruit\": \"banana\"\n  },\n  {\n    \"_id\": \"6530d96bdd1e27f2847a41b7\",\n    \"index\": 1,\n    \"guid\": \"c353117f-3e97-4052-8189-53971370bea0\",\n    \"isActive\": false,\n    \"balance\": \"$3,921.66\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 36,\n    \"eyeColor\": \"brown\",\n    \"name\": \"Socorro Delacruz\",\n    \"gender\": \"female\",\n    \"company\": \"EYERIS\",\n    \"email\": \"socorrodelacruz@eyeris.com\",\n    \"phone\": \"+1 (812) 464-2296\",\n    \"address\": \"849 Bank Street, Madaket, New Mexico, 5302\",\n    \"about\": \"Nisi aute duis minim tempor cupidatat elit quis nisi adipisicing esse ipsum pariatur sunt. Incididunt velit velit ullamco occaecat non pariatur consectetur. Occaecat magna tempor id et consequat sit ipsum voluptate sunt non velit fugiat Lorem ex. Deserunt aute ut exercitation laborum aute reprehenderit veniam consequat laboris fugiat officia aute sunt quis. Elit voluptate ad veniam laboris nisi aliquip dolor enim aute mollit tempor. Duis ex aliquip sit culpa commodo et culpa. Lorem in reprehenderit consectetur adipisicing aute id.\\r\\n\",\n    \"registered\": \"2018-06-08T09:30:58 -03:00\",\n    \"latitude\": -63.990273,\n    \"longitude\": -177.365978,\n    \"tags\": [\n      \"non\",\n      \"commodo\",\n      \"sint\",\n      \"nisi\",\n      \"laboris\",\n      \"in\",\n      \"fugiat\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Lauren Livingston\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Erna Maynard\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Crystal Church\"\n      }\n    ],\n    \"greeting\": \"Hello, Socorro Delacruz! You have 3 unread messages.\",\n    \"favoriteFruit\": \"apple\"\n  },\n  {\n    \"_id\": \"6530d96b4fa4b866b370aa63\",\n    \"index\": 2,\n    \"guid\": \"af05e6e8-06e9-43a7-91fa-d51bd470d4d3\",\n    \"isActive\": false,\n    \"balance\": \"$2,721.27\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 39,\n    \"eyeColor\": \"green\",\n    \"name\": \"Debbie Benson\",\n    \"gender\": \"female\",\n    \"company\": \"ACIUM\",\n    \"email\": \"debbiebenson@acium.com\",\n    \"phone\": \"+1 (924) 579-2622\",\n    \"address\": \"598 King Street, Edgewater, Hawaii, 843\",\n    \"about\": \"Sit dolor ullamco elit velit do. Ex cillum culpa aliqua ut fugiat. Veniam irure est minim amet cupidatat laborum ad elit sit culpa mollit do. Qui sit commodo nisi aute sint laborum proident ut exercitation tempor Lorem exercitation excepteur. Consequat in in deserunt minim.\\r\\n\",\n    \"registered\": \"2020-03-28T12:30:27 -03:00\",\n    \"latitude\": 32.696383,\n    \"longitude\": 120.139506,\n    \"tags\": [\n      \"officia\",\n      \"amet\",\n      \"esse\",\n      \"consequat\",\n      \"duis\",\n      \"nulla\",\n      \"aute\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Long Mccormick\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Alana Horne\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Kathleen Haynes\"\n      }\n    ],\n    \"greeting\": \"Hello, Debbie Benson! You have 1 unread messages.\",\n    \"favoriteFruit\": \"banana\"\n  },\n  {\n    \"_id\": \"6530d96bfb424edf488996bc\",\n    \"index\": 3,\n    \"guid\": \"0b749df5-bb2e-45e1-8f9e-e56c2ebded50\",\n    \"isActive\": false,\n    \"balance\": \"$3,900.53\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 24,\n    \"eyeColor\": \"blue\",\n    \"name\": \"Cervantes Stone\",\n    \"gender\": \"male\",\n    \"company\": \"LOCAZONE\",\n    \"email\": \"cervantesstone@locazone.com\",\n    \"phone\": \"+1 (913) 517-3407\",\n    \"address\": \"168 Hutchinson Court, Calpine, South Carolina, 1338\",\n    \"about\": \"Qui duis excepteur do consectetur dolore id in qui aliquip elit laborum dolor nulla nulla. Mollit magna ut fugiat magna id est non laborum aute non incididunt. Et exercitation est est deserunt do. Eiusmod adipisicing consectetur ullamco veniam exercitation cillum ad velit est. Velit exercitation Lorem aute incididunt irure officia pariatur duis reprehenderit ad commodo. Labore velit in duis labore pariatur laboris duis commodo velit aliqua commodo.\\r\\n\",\n    \"registered\": \"2017-07-16T01:33:27 -03:00\",\n    \"latitude\": 63.81319,\n    \"longitude\": 143.20968,\n    \"tags\": [\n      \"velit\",\n      \"labore\",\n      \"in\",\n      \"duis\",\n      \"amet\",\n      \"pariatur\",\n      \"occaecat\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Muriel Cobb\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Bartlett Blevins\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Alison Parrish\"\n      }\n    ],\n    \"greeting\": \"Hello, Cervantes Stone! You have 2 unread messages.\",\n    \"favoriteFruit\": \"strawberry\"\n  },\n  {\n    \"_id\": \"6530d96b6abc0fd6205b2931\",\n    \"index\": 4,\n    \"guid\": \"7af01846-5852-47cf-809a-9d7cff0a5f33\",\n    \"isActive\": false,\n    \"balance\": \"$2,612.30\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 27,\n    \"eyeColor\": \"brown\",\n    \"name\": \"Elsie Hobbs\",\n    \"gender\": \"female\",\n    \"company\": \"BOINK\",\n    \"email\": \"elsiehobbs@boink.com\",\n    \"phone\": \"+1 (983) 589-3960\",\n    \"address\": \"102 Cropsey Avenue, Frierson, Kansas, 1379\",\n    \"about\": \"Voluptate fugiat cillum eiusmod Lorem elit tempor adipisicing aute. Elit sunt laborum deserunt est ipsum sunt aute enim ex ullamco. Ullamco veniam nulla ex id anim ullamco labore. Nisi in Lorem laborum in duis excepteur voluptate sint culpa velit laborum incididunt.\\r\\n\",\n    \"registered\": \"2022-06-12T06:02:07 -03:00\",\n    \"latitude\": -83.164798,\n    \"longitude\": 144.735765,\n    \"tags\": [\n      \"id\",\n      \"nostrud\",\n      \"do\",\n      \"quis\",\n      \"consequat\",\n      \"dolore\",\n      \"irure\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Jacqueline Salazar\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Callahan Chavez\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Hull Callahan\"\n      }\n    ],\n    \"greeting\": \"Hello, Elsie Hobbs! You have 6 unread messages.\",\n    \"favoriteFruit\": \"banana\"\n  },\n  {\n    \"_id\": \"6530d96b6ccdb5a05ac42d89\",\n    \"index\": 5,\n    \"guid\": \"9270e977-09b1-49ff-a089-3daf6b0f6638\",\n    \"isActive\": false,\n    \"balance\": \"$1,178.38\",\n    \"picture\": \"http://placehold.it/32x32\",\n    \"age\": 32,\n    \"eyeColor\": \"green\",\n    \"name\": \"Flossie Higgins\",\n    \"gender\": \"female\",\n    \"company\": \"ASSISTIX\",\n    \"email\": \"flossiehiggins@assistix.com\",\n    \"phone\": \"+1 (920) 559-2423\",\n    \"address\": \"373 Navy Street, Kohatk, South Dakota, 9646\",\n    \"about\": \"Enim nulla culpa nostrud dolor quis et culpa consequat exercitation in deserunt. Officia voluptate nulla laborum sunt et incididunt ut sint quis. Eiusmod enim tempor irure amet aute ipsum culpa esse id cupidatat ea. Occaecat laboris proident consequat ullamco. Ea est nostrud aliqua eiusmod amet quis. Non dolor pariatur dolor incididunt aliquip occaecat adipisicing fugiat pariatur Lorem duis enim voluptate elit.\\r\\n\",\n    \"registered\": \"2014-10-30T10:11:50 -02:00\",\n    \"latitude\": 61.493115,\n    \"longitude\": 102.592193,\n    \"tags\": [\n      \"pariatur\",\n      \"et\",\n      \"exercitation\",\n      \"cupidatat\",\n      \"sit\",\n      \"anim\",\n      \"consequat\"\n    ],\n    \"friends\": [\n      {\n        \"id\": 0,\n        \"name\": \"Daphne Velasquez\"\n      },\n      {\n        \"id\": 1,\n        \"name\": \"Melanie Leon\"\n      },\n      {\n        \"id\": 2,\n        \"name\": \"Young Carter\"\n      }\n    ],\n    \"greeting\": \"Hello, Flossie Higgins! You have 5 unread messages.\",\n    \"favoriteFruit\": \"strawberry\"\n  }\n]"; 1_000_000];

        for appendable in appendables {
            let append_result = write_queue.append(appendable).await;
            assert!(append_result.is_ok());
        }

        // let cleanup_result = cleanup_test_file(&test_file_path).await;

        // assert!(cleanup_result.is_ok());
    }
}
