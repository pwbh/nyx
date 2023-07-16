#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Topic {
    id: String,
    name: String,
}

impl Topic {
    pub fn from(id: String, name: String) -> Result<Self, String> {
        Ok(Self { id, name })
    }
}
