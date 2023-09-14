pub mod command_processor;
pub mod config;
pub mod distribution_manager;

use std::sync::{Arc, Mutex};

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
    pub listener: tokio::net::TcpListener,
    pub distribution_manager: Arc<Mutex<DistributionManager>>,
    pub command_processor: CommandProcessor,
    pub system: System,
}

impl Observer {
    pub async fn from(
        config_path: &str,
        leader: Option<&String>,
        name: Option<&String>,
    ) -> Result<Self, String> {
        let role = if leader.is_none() {
            Role::Leader
        } else {
            Role::Follower
        };

        let mut system = System::new_all();

        let port: u16 = if let Ok(port) = std::env::var("PORT") {
            port.parse::<u16>().unwrap_or(DEFAULT_PORT)
        } else {
            DEFAULT_PORT
        };

        let config = Config::from(config_path.into())?;

        let distribution_manager = DistributionManager::from(config, name)?;

        let command_processor = CommandProcessor::new();

        let host = format!("localhost:{}", port);

        let listener = tokio::net::TcpListener::bind(host)
            .await
            .map_err(|e| e.to_string())?;

        system.refresh_all();

        let mut total_disk_utilization: f64 = 0.0;

        // Display disk status
        for disk in system.disks() {
            total_disk_utilization += disk.total_space() as f64
        }

        total_disk_utilization =
            (total_disk_utilization / system.disks().len() as f64) * 1.0 * 10f64.powf(-9.0);

        println!("Disk space: {:.2} GiB", total_disk_utilization);
        let mut total_cpu_utilization = 0f32;

        for cpu in system.cpus() {
            total_cpu_utilization += cpu.cpu_usage();
        }

        total_cpu_utilization /= system.cpus().len() as f32;

        println!("CPU utilization: {:.1}%", total_cpu_utilization);

        let observer = Self {
            id: Uuid::new_v4().to_string(),
            role,
            distribution_manager,
            command_processor,
            listener,
            system,
        };

        Ok(observer)
    }
}
