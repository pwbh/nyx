use std::{io::Write, net::TcpStream, sync::MutexGuard};

use shared_structures::Status;

use crate::distribution_manager::{Broker, Partition};

pub enum Message<'a> {
    CreatePartition(&'a Partition),
}

pub struct Broadcast;

impl Broadcast {
    pub fn broadcast(streams: &mut [TcpStream], message: Message) -> Result<(), String> {
        for stream in streams {
            stream.write("test".as_bytes()).map_err(|e| e.to_string())?;
        }

        Ok(())
    }

    pub fn broadcast_to(stream: &mut TcpStream, message: Message) -> Result<(), String> {
        stream.write("test".as_bytes()).map_err(|e| e.to_string())?;
        Ok(())
    }

    // This will broadcast to every broker the necessary command for the broker to execute the necessary command
    // here its Message::CreatePartition, with the relevant data
    pub fn create_partition(
        brokers_lock: &mut MutexGuard<'_, Vec<Broker>>,
        partition_id: &str,
    ) -> Result<(), String> {
        for broker in brokers_lock.iter_mut() {
            for p in broker.partitions.iter_mut() {
                if p.id == partition_id {
                    Self::broadcast_to(&mut broker.stream, Message::CreatePartition(&p))?;
                    // After successful creation of the partition on the broker,
                    // we can set its status on the observer to Active.
                    p.status = Status::Active;
                }
            }
        }

        Ok(())
    }
}
