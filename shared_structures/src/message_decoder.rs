use crate::Message;

pub struct MessageDecoder;

impl MessageDecoder {
    pub fn decode(raw_message: &str) -> Result<Message, String> {
        serde_json::from_str::<Message>(&raw_message)
            .map_err(|e| format!("Error while deserialziing: {}", e))
    }
}
