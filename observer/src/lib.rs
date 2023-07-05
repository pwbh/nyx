use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
};

use command_processor::CommandProcessor;
use distribution_manager::DistributionManager;
use shared_structures::Role;
use uuid::Uuid;

pub mod command_processor;
pub mod distribution_manager;
mod utils;

pub struct Observer {
    id: String,
    role: Role,
    replica_number: u32,
    pub listener: TcpListener,
    pub distribution_manager: Arc<Mutex<DistributionManager>>,
    pub command_processor: CommandProcessor,
}

impl Observer {
    pub fn new(role: Role, replica_number: u32) -> Result<Self, String> {
        let distribution_manager = DistributionManager::new();
        let command_processor = CommandProcessor::new();

        let listener = TcpListener::bind("localhost:3000").map_err(|e| e.to_string())?;

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            role,
            replica_number,
            distribution_manager,
            command_processor,
            listener,
        })
    }
}
