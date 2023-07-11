use std::sync::MutexGuard;

use shared_structures::Status;

use crate::distribution_manager::{Broker, Partition};

pub enum Message<'a> {
    CreatePartition(&'a Partition),
}

pub struct Communication;

impl Communication {
    fn broadcast(&self, message: Message) -> Result<(), String> {
        Ok(())
    }

    // This will broadcast to every broker the necessary command for the broker to execute the necessary command
    // here its Message::CreatePartition, with the relevant data
    pub fn broadcast_partition<'a>(
        &self,
        brokers_lock: &mut MutexGuard<'_, Vec<Broker>>,
        partition_id: &str,
    ) -> Result<(), String> {
        for broker in brokers_lock.iter_mut() {
            for p in broker.partitions.iter_mut() {
                if p.id == partition_id {
                    self.broadcast(Message::CreatePartition(&p))?;
                    // After successful creation of the partition on the broker,
                    // we can set its status on the observer to Active.
                    p.status = Status::Active;
                }
            }
        }

        Ok(())
    }
}
