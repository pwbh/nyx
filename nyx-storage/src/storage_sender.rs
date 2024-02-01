use async_std::channel::Sender;

pub struct StorageSender {
    inner: Sender<Vec<u8>>,
}

impl StorageSender {
    pub fn new(inner: Sender<Vec<u8>>) -> Self {
        Self { inner }
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<(), String> {
        self.inner
            .send(data.to_vec())
            .await
            .map_err(|e| format!("StorageSender (send): {}", e))
    }
}

#[cfg(test)]
mod tests {
    use async_std::channel::bounded;

    use super::*;

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn create_storage_sender_and_send() {
        let (sender, receiver) = bounded::<Vec<u8>>(1);
        let payload = b"testing storage sender";
        let mut storage_sender = StorageSender::new(sender);
        let send_result = storage_sender.send(payload).await;

        assert!(send_result.is_ok());

        let received_data = receiver.recv().await.unwrap();

        assert_eq!(
            String::from_utf8(received_data).unwrap(),
            String::from_utf8(payload.to_vec()).unwrap()
        )
    }
}
