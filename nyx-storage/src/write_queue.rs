use async_std::channel::Receiver;

pub struct WriteQueue;

impl WriteQueue {
    pub async fn run(queue: Receiver<Vec<u8>>) -> Result<(), String> {
        while let Ok(data) = queue.recv().await {
            println!("Do something with received data: {:?}", data);
        }

        Ok(())
    }
}
