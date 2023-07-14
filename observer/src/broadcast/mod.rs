use std::{io::Write, net::TcpStream, sync::MutexGuard};

use shared_structures::{Message, Status};

use crate::distribution_manager::Broker;

pub struct Broadcast;

impl Broadcast {
    pub fn all<T: serde::Serialize>(
        streams: &mut [TcpStream],
        message: Message<T>,
    ) -> Result<(), String> {
        for stream in streams {
            let payload = serde_json::to_string(&message)
                .map_err(|_| "Couldn't serialize the data structure to send.".to_string())?;
            let bytes_written = stream
                .write(payload.as_bytes())
                .map_err(|e| e.to_string())?;
            if bytes_written == 0 {
                return Err("0 bytes has been written, might be an error, plesae check".to_string());
            }
        }
        Ok(())
    }

    pub fn to<T: serde::Serialize>(
        stream: &mut TcpStream,
        message: Message<T>,
    ) -> Result<(), String> {
        let payload = serde_json::to_string(&message)
            .map_err(|_| "Couldn't serialize the data structure to send.".to_string())?;
        println!("Payload {:?}", payload);
        let bytes_written = stream
            .write(payload.as_bytes())
            .map_err(|e| e.to_string())?;
        if bytes_written == 0 {
            return Err("0 bytes has been written, might be an error, plesae check".to_string());
        }
        Ok(())
    }

    // This will broadcast to every broker the necessary command for the broker to execute the necessary command
    // here its Message::CreatePartition, with the relevant data
    pub fn create_partition(
        brokers_lock: &mut MutexGuard<'_, Vec<Broker>>,
        partition_id: &str,
    ) -> Result<(), String> {
        println!("CREATING PARTITION");
        for broker in brokers_lock.iter_mut() {
            for p in broker.partitions.iter_mut() {
                if p.id == partition_id {
                    Self::to(&mut broker.stream, Message::CreatePartition(p.clone()))?;
                    // After successful creation of the partition on the broker,
                    // we can set its status on the observer to Active.
                    p.status = Status::Active;
                }
            }
        }
        Ok(())
    }
}
