#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Record {
    payload: serde_json::Value,
}

impl Record {
    pub fn new(payload: serde_json::Value) -> Self {
        Self { payload }
    }
}
