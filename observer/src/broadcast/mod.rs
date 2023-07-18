use std::{io::Write, net::TcpStream};

use shared_structures::{Message, Status};

use crate::distribution_manager::{Broker, Partition};

pub struct Broadcast;

impl Broadcast {
    pub fn all<T: serde::Serialize>(
        streams: &mut [TcpStream],
        message: Message<T>,
    ) -> Result<(), String> {
        for stream in streams {
            let mut payload = serde_json::to_string(&message)
                .map_err(|_| "Couldn't serialize the data structure to send.".to_string())?;

            payload.push('\n');

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
    pub fn create_partition(broker: &mut Broker, partition_id: &str) -> Result<(), String> {
        for p in broker.partitions.iter_mut() {
            if p.id == partition_id {
                Self::to(&mut broker.stream, Message::CreatePartition(p.clone()))?;
                // After successful creation of the partition on the broker,
                // we can set its status on the observer to Active.
                p.status = Status::Up;
            }
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
