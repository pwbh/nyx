pub mod command_processor;
pub mod config;
pub mod distribution_manager;

use std::{
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use command_processor::CommandProcessor;
use config::Config;
use distribution_manager::DistributionManager;
use shared_structures::Role;
use sysinfo::{CpuExt, DiskExt, System, SystemExt};
use uuid::Uuid;

pub const DEV_CONFIG: &str = "dev.properties";
pub const PROD_CONFIG: &str = "prod.properties";

const DEFAULT_PORT: u16 = 2828;

pub const CLUSTER_FILE: &str = "cluster.json";

pub struct Observer {
    pub id: String,
    pub role: Role,
    pub listener: TcpListener,
    pub distribution_manager: Arc<Mutex<DistributionManager>>,
    pub command_processor: CommandProcessor,
    pub system: System,
    pub leader_stream: Option<TcpStream>,
}

impl Observer {
    pub fn from(config_path: &str, role: Role, name: Option<&String>) -> Result<Self, String> {
        let mut system = System::new_all();

        let port: u16 = if let Ok(port) = std::env::var("PORT") {
            port.parse::<u16>().unwrap_or(DEFAULT_PORT)
        } else {
            DEFAULT_PORT
        };

        let config = Config::from(config_path.into())?;

        let distribution_manager = DistributionManager::new(config, name)?;

        let command_processor = CommandProcessor::new();

        let host = format!("localhost:{}", port);

        let listener = TcpListener::bind(host).map_err(|e| e.to_string())?;

        system.refresh_all();

        let mut total_disk_utilization: f64 = 0.0;

        // Display disk status
        for disk in system.disks() {
            total_disk_utilization += disk.total_space() as f64
        }

        total_disk_utilization =
            (total_disk_utilization / system.disks().len() as f64) * 1.0 * 10f64.powf(-9.0);

        println!("Total disk space: {:.2} GiB", total_disk_utilization);
        let mut total_cpu_utilization = 0f32;

        for cpu in system.cpus() {
            total_cpu_utilization += cpu.cpu_usage();
        }

        total_cpu_utilization /= system.cpus().len() as f32;

        println!("Total CPU utilization: {:.1}%", total_cpu_utilization);

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            role,
            distribution_manager,
            command_processor,
            listener,
            system,
            leader_stream: None,
        })
    }
}
