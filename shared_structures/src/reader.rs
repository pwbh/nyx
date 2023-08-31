use std::{io::Read, net::TcpStream};

use crate::Message;

pub struct Reader;

impl Reader {
    pub fn read_one_message(stream: &mut TcpStream) -> Result<Message, String> {
        let message = Self::read_until_char(stream, '\n')?;

        serde_json::from_str::<Message>(&message)
            .map_err(|e| format!("Error while deserialziing: {}", e))
    }

    fn read_until_char(stream: &mut TcpStream, target_char: char) -> Result<String, String> {
        let mut buffer = [0u8; 1]; // Read one byte at a time
        let mut result = String::new();

        loop {
            stream.read_exact(&mut buffer).map_err(|e| e.to_string())?; // Read one byte into the buffer

            let byte_read = buffer[0];
            let character = byte_read as char;
            result.push(character);

            if character == target_char {
                break;
            }
        }

        Ok(result)
    }
}
