pub mod command_processor;
pub mod config;
pub mod distribution_manager;

mod utils;

use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
};

use command_processor::CommandProcessor;
use config::Config;
use distribution_manager::DistributionManager;
use shared_structures::Role;
use uuid::Uuid;

pub const DEV_CONFIG: &str = "dev.properties";
pub const PROD_CONFIG: &str = "prod.properties";

pub struct Observer {
    id: String,
    role: Role,
    replica_number: u32,
    pub listener: TcpListener,
    pub distribution_manager: Arc<Mutex<DistributionManager>>,
    pub command_processor: CommandProcessor,
}

impl Observer {
    pub fn new(config_path: &str, role: Role, replica_number: u32) -> Result<Self, String> {
        let config = Config::from(config_path.into())?;

        println!("{:?}", config);

        let replica_factor = config
            .get_number("replica_factor")
            .ok_or("Replica factor is missing from config.".to_string())?;

        println!("{}", replica_factor);

        let distribution_manager = DistributionManager::new(config);
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
