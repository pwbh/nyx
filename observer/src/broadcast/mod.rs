use std::{io::Write, net::TcpStream};

use shared_structures::{Message, Status};

use crate::distribution_manager::{Broker, Partition};

pub struct Broadcast;

impl Broadcast {
    pub fn all<T: serde::Serialize>(
        streams: &mut [&mut TcpStream],
        message: &Message<T>,
    ) -> Result<(), String> {
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

    pub fn to<T: serde::Serialize>(
        stream: &mut TcpStream,
        message: Message<T>,
    ) -> Result<(), String> {
        let mut payload = serde_json::to_string(&message)
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

    // This will broadcast to every broker the necessary command for the broker to execute the necessary command
    // here its Message::CreatePartition, with the relevant data
    pub fn replicate_partition(broker: &mut Broker, replica: &mut Partition) -> Result<(), String> {
        Self::to(
            &mut broker.stream,
            Message::CreatePartition(replica.clone()),
        )?;
        // After successful creation of the partition on the broker,
        // we can set its status on the observer to Active.
        replica.status = Status::Up;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Read,
        net::TcpListener,
        sync::{Arc, Mutex},
    };

    use super::*;

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    struct TestMessage {
        name: String,
        age: usize,
    }

    #[test]
    fn all_broadcasts_messages_to_everyone() {
        let listener = TcpListener::bind("localhost:15000").unwrap();

        let client_to_server_stream_one: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
        let client_to_server_stream_one_thread = client_to_server_stream_one.clone();

        std::thread::spawn(move || {
            let stream = TcpStream::connect("localhost:15000").unwrap();
            let mut lock = client_to_server_stream_one_thread.lock().unwrap();
            *lock = Some(stream);
        });

        let client_to_server_stream_two: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
        let client_to_server_stream_two_thread = client_to_server_stream_two.clone();

        std::thread::spawn(move || {
            let stream = TcpStream::connect("localhost:15000").unwrap();
            let mut lock = client_to_server_stream_two_thread.lock().unwrap();
            *lock = Some(stream);
        });

        let client_to_server_stream_three: Arc<Mutex<Option<TcpStream>>> =
            Arc::new(Mutex::new(None));
        let client_to_server_stream_three_thread = client_to_server_stream_three.clone();

        std::thread::spawn(move || {
            let stream = TcpStream::connect("localhost:15000").unwrap();
            let mut lock = client_to_server_stream_three_thread.lock().unwrap();
            *lock = Some(stream);
        });

        let (mut server_to_client_stream_one, _) = listener.accept().unwrap();
        let (mut server_to_client_stream_two, _) = listener.accept().unwrap();
        let (mut server_to_client_stream_three, _) = listener.accept().unwrap();

        let mut streams = [
            &mut server_to_client_stream_one,
            &mut server_to_client_stream_two,
            &mut server_to_client_stream_three,
        ];

        let test_message = TestMessage {
            name: "Nyx Better Then Kafka".to_string(),
            age: 0,
        };

        let payload = Message::Any(test_message.clone());

        let result = Broadcast::all(&mut streams, &payload);

        assert!(result.is_ok());

        // Closing the streams so we can read to EOF later
        for stream in streams {
            stream.shutdown(std::net::Shutdown::Both).unwrap();
        }

        let mut buf = String::with_capacity(1024);

        {
            let lock = client_to_server_stream_one.lock().unwrap();
            let result = lock.as_ref().unwrap().read_to_string(&mut buf).unwrap();
            let data: Message<TestMessage> =
                serde_json::from_str::<Message<TestMessage>>(buf.trim()).unwrap();
            assert!(result > 0);
            assert_eq!(
                data,
                Message::Any(TestMessage {
                    ..test_message.clone()
                })
            );
            buf.clear();
        }

        {
            let lock = client_to_server_stream_two.lock().unwrap();
            let result = lock.as_ref().unwrap().read_to_string(&mut buf).unwrap();
            let data: Message<TestMessage> =
                serde_json::from_str::<Message<TestMessage>>(buf.trim()).unwrap();
            assert!(result > 0);
            assert_eq!(
                data,
                Message::Any(TestMessage {
                    ..test_message.clone()
                })
            );
            buf.clear();
        }

        {
            let lock = client_to_server_stream_three.lock().unwrap();
            let result = lock.as_ref().unwrap().read_to_string(&mut buf).unwrap();
            let data: Message<TestMessage> =
                serde_json::from_str::<Message<TestMessage>>(buf.trim()).unwrap();
            assert!(result > 0);
            assert_eq!(
                data,
                Message::Any(TestMessage {
                    ..test_message.clone()
                })
            );
            buf.clear();
        }
    }
}
