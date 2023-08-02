pub mod command_processor;
pub mod config;
pub mod distribution_manager;

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

const DEFAULT_PORT: u16 = 2828;

pub struct Observer {
    pub id: String,
    pub role: Role,
    pub listener: TcpListener,
    pub distribution_manager: Arc<Mutex<DistributionManager>>,
    pub command_processor: CommandProcessor,
}

impl Observer {
    pub fn new(config_path: &str, role: Role) -> Result<Self, String> {
        let port: u16 = if let Ok(port) = std::env::var("PORT") {
            port.parse::<u16>().unwrap_or(DEFAULT_PORT)
        } else {
            DEFAULT_PORT
        };

        let config = Config::from(config_path.into())?;

        let distribution_manager = DistributionManager::new(config);

        let command_processor = CommandProcessor::new();

        let host = format!("localhost:{}", port);

        let listener = TcpListener::bind(host).map_err(|e| e.to_string())?;

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            role,
            distribution_manager,
            command_processor,
            listener,
        })
    }
}
