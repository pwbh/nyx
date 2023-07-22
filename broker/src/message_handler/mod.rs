use shared_structures::Message;

pub struct MessageHandler;

impl MessageHandler {
    pub fn handle_incoming_message<'a, T: serde::Serialize + serde::Deserialize<'a>>(
        raw_data: &'a str,
    ) -> Result<Message, String> {
        serde_json::from_str(raw_data).map_err(|e| e.to_string())
    }
}
