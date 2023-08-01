use std::net::TcpStream;

use shared_structures::{Message, Status, Topic};

use crate::{Broker, Partition};

pub struct MessageHandler<'a> {
    observer_stream: TcpStream,
    broker: &'a mut Broker,
}

impl<'a> MessageHandler<'a> {
    pub fn from(broker: &'a mut Broker) -> Result<MessageHandler, String> {
        let observer_stream = broker.stream.try_clone().map_err(|e| e.to_string())?;
        Ok(Self {
            observer_stream,
            broker,
        })
    }

    pub fn handle_raw_message(&mut self, raw_data: &str) -> Result<(), String> {
        let message = serde_json::from_str::<Message>(raw_data).map_err(|e| e.to_string())?;
        self.handle_by_message(&message)
    }

    fn handle_by_message(&mut self, message: &Message) -> Result<(), String> {
        match message {
            Message::CreatePartition {
                id,
                replica_id,
                topic,
            } => self.handle_create_partition(id, replica_id, topic),
            Message::ProducerWantsToConnect { topic } => {
                println!("Producer wants to connect to topic `{}`", topic);
                // TODO: Should send back data with the locations (hosts) that hold the partition for given topic.
                Ok(())
            }
            Message::ClusterMetadata { metadata } => {
                println!("New metadata received from the cluster: {:#?}", metadata);
                self.broker.cluster_metadata = metadata.clone();
                Ok(())
            }
            _ => Err(format!(
                "Message {:?} is not handled in `handle_by_message`.",
                message
            )),
        }
    }

    fn handle_create_partition(
        &mut self,
        id: &String,
        replica_id: &String,
        topic: &Topic,
    ) -> Result<(), String> {
        println!("{:?}", self.broker.custom_dir);
        let partition = Partition::from(
            id.clone(),
            replica_id.clone(),
            Status::Up,
            topic.clone(),
            shared_structures::Role::Follower,
            1,
            1,
            self.broker.custom_dir.as_ref(),
        )?;
        self.broker.create_partition(partition)
    }
}
