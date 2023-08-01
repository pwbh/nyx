use std::{io::Write, net::TcpStream};

use crate::Message;

pub struct Broadcast;

impl Broadcast {
    pub fn all(streams: &mut [&mut TcpStream], message: &Message) -> Result<(), String> {
        let mut payload = serde_json::to_string(message)
            .map_err(|_| "Couldn't serialize the data structure to send.".to_string())?;

        payload.push('\n');

        for stream in streams.iter_mut() {
            let bytes_written = stream
                .write(payload.as_bytes())
                .map_err(|e| e.to_string())?;

            if bytes_written == 0 {
                return Err(
                    "0 bytes have been written, might be an error, please create a new issue in nyx repository.".to_string()
                );
            }
        }
        Ok(())
    }

    pub fn to(stream: &mut TcpStream, message: &Message) -> Result<(), String> {
        let mut payload = serde_json::to_string(message)
            .map_err(|_| "Couldn't serialize the data structure to send.".to_string())?;

        payload.push('\n');

        let bytes_written = stream
            .write(payload.as_bytes())
            .map_err(|e| e.to_string())?;

        println!("Written bytes: {}", bytes_written);

        if bytes_written == 0 {
            return Err("0 bytes have been written, might be an error, please create a new issue in nyx repository.".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Read, net::TcpListener};

    use crate::Topic;

    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn all_broadcasts_messages_to_everyone() {
        let listener = TcpListener::bind("localhost:15000").unwrap();

        let thread1 = std::thread::spawn(|| TcpStream::connect("localhost:15000").unwrap());

        let thread2 = std::thread::spawn(|| TcpStream::connect("localhost:15000").unwrap());

        let thread3 = std::thread::spawn(|| TcpStream::connect("localhost:15000").unwrap());

        let (mut server_to_client_stream_one, _) = listener.accept().unwrap();
        let (mut server_to_client_stream_two, _) = listener.accept().unwrap();
        let (mut server_to_client_stream_three, _) = listener.accept().unwrap();

        let mut client_to_server_stream_one = thread1.join().unwrap();
        let mut client_to_server_stream_two = thread2.join().unwrap();
        let mut client_to_server_stream_three = thread3.join().unwrap();

        let mut streams = [
            &mut server_to_client_stream_one,
            &mut server_to_client_stream_two,
            &mut server_to_client_stream_three,
        ];

        let test_message = Message::CreatePartition {
            id: uuid::Uuid::new_v4().to_string(),
            replica_id: uuid::Uuid::new_v4().to_string(),
            topic: Topic::from("notifications".to_string()),
            replica_count: 1,
            partition_number: 1,
        };

        let result = Broadcast::all(&mut streams, &test_message);

        assert!(result.is_ok());

        // Closing the streams so we can read to EOF later
        for stream in streams {
            stream.shutdown(std::net::Shutdown::Both).unwrap();
        }

        let mut buf = String::with_capacity(1024);

        {
            let result = client_to_server_stream_one
                .read_to_string(&mut buf)
                .unwrap();
            let data: Message = serde_json::from_str::<Message>(buf.trim()).unwrap();
            assert!(result > 0);
            assert!(matches!(data, Message::CreatePartition { .. }));
            buf.clear();
        }

        {
            let result = client_to_server_stream_two
                .read_to_string(&mut buf)
                .unwrap();
            let data: Message = serde_json::from_str::<Message>(buf.trim()).unwrap();
            assert!(result > 0);
            assert!(matches!(data, Message::CreatePartition { .. }));
            buf.clear();
        }

        {
            let result = client_to_server_stream_three
                .read_to_string(&mut buf)
                .unwrap();
            let data: Message = serde_json::from_str::<Message>(buf.trim()).unwrap();
            assert!(result > 0);
            assert!(matches!(data, Message::CreatePartition { .. }));
            buf.clear();
        }
    }
}
