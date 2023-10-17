use async_std::channel::Sender;

pub struct StorageSender {
    inner: Sender<Vec<u8>>,
}

impl StorageSender {
    pub fn new(inner: Sender<Vec<u8>>) -> Self {
        Self { inner }
    }

    pub async fn send(&mut self, data: Vec<u8>) -> Result<(), String> {
        self.inner
            .send(data)
            .await
            .map_err(|e| format!("StorageSender (send): {}", e))
    }
}
