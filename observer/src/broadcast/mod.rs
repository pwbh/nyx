use std::{io::Write, net::TcpStream};

use shared_structures::{Message, Status};

use crate::distribution_manager::{Broker, Partition};

pub struct Broadcast;

impl Broadcast {
    pub fn all(streams: &mut [TcpStream], message: &Message) -> Result<(), String> {
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

    pub fn to(stream: &mut TcpStream, message: Message) -> Result<(), String> {
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
            Message::CreatePartition {
                id: replica.id.clone(),
                replica_id: replica.replica_id.clone(),
                topic: replica.topic.lock().unwrap().clone(),
            },
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

    use shared_structures::Topic;

    use super::*;

    #[test]
    fn all_broadcasts_messages_to_everyone() {
        let listener = TcpListener::bind("localhost:15000").unwrap();

        let client_to_server_stream_one: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
        let client_to_server_stream_one_thread = client_to_server_stream_one.clone();

        let thread1 = std::thread::spawn(move || {
            let stream = TcpStream::connect("localhost:15000").unwrap();
            let mut lock = client_to_server_stream_one_thread.lock().unwrap();
            *lock = Some(stream);
        });

        let client_to_server_stream_two: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
        let client_to_server_stream_two_thread = client_to_server_stream_two.clone();

        let thread2 = std::thread::spawn(move || {
            let stream = TcpStream::connect("localhost:15000").unwrap();
            let mut lock = client_to_server_stream_two_thread.lock().unwrap();
            *lock = Some(stream);
        });

        let client_to_server_stream_three: Arc<Mutex<Option<TcpStream>>> =
            Arc::new(Mutex::new(None));
        let client_to_server_stream_three_thread = client_to_server_stream_three.clone();

        let thread3 = std::thread::spawn(move || {
            let stream = TcpStream::connect("localhost:15000").unwrap();
            let mut lock = client_to_server_stream_three_thread.lock().unwrap();
            *lock = Some(stream);
        });

        let (server_to_client_stream_one, _) = listener.accept().unwrap();
        let (server_to_client_stream_two, _) = listener.accept().unwrap();
        let (server_to_client_stream_three, _) = listener.accept().unwrap();

        thread1.join().unwrap();
        thread2.join().unwrap();
        thread3.join().unwrap();

        let mut streams = [
            server_to_client_stream_one,
            server_to_client_stream_two,
            server_to_client_stream_three,
        ];

        let test_message = Message::CreatePartition {
            id: uuid::Uuid::new_v4().to_string(),
            replica_id: uuid::Uuid::new_v4().to_string(),
            topic: Topic::from("notifications".to_string()),
        };

        let result = Broadcast::all(&mut streams, &test_message);

        assert!(result.is_ok());

        // Closing the streams so we can read to EOF later
        for stream in streams {
            stream.shutdown(std::net::Shutdown::Both).unwrap();
        }

        let mut buf = String::with_capacity(1024);

        {
            let lock = client_to_server_stream_one.lock().unwrap();
            let result = lock.as_ref().unwrap().read_to_string(&mut buf).unwrap();
            let data: Message = serde_json::from_str::<Message>(buf.trim()).unwrap();
            assert!(result > 0);
            assert!(matches!(data, Message::CreatePartition { .. }));
            buf.clear();
        }

        {
            let lock = client_to_server_stream_two.lock().unwrap();
            let result = lock.as_ref().unwrap().read_to_string(&mut buf).unwrap();
            let data: Message = serde_json::from_str::<Message>(buf.trim()).unwrap();
            assert!(result > 0);
            assert!(matches!(data, Message::CreatePartition { .. }));
            buf.clear();
        }

        {
            let lock = client_to_server_stream_three.lock().unwrap();
            let result = lock.as_ref().unwrap().read_to_string(&mut buf).unwrap();
            let data: Message = serde_json::from_str::<Message>(buf.trim()).unwrap();
            assert!(result > 0);
            assert!(matches!(data, Message::CreatePartition { .. }));
            buf.clear();
        }
    }
}
