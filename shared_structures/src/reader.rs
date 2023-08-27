use std::io::{BufRead, BufReader};

pub struct Reader;

impl Reader {
    pub fn read_message<R: std::io::Read, T: serde::de::DeserializeOwned>(
        inner: R,
    ) -> Result<T, String> {
        let mut reader = BufReader::new(inner);
        let mut buf = String::with_capacity(1024);
        let message = reader
            .read_line(&mut buf)
            .map_err(|e| format!("Reader error: {}", e))?;

        if message == 0 {
            return Err("Could not read message from given value".to_string());
        }

        serde_json::from_str::<T>(&buf).map_err(|e| format!("Error while deserialziing: {}", e))
    }
}
