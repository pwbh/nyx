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
use sysinfo::{CpuExt, NetworkExt, System, SystemExt};
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
    pub system: System,
}

impl Observer {
    pub fn new(config_path: &str, role: Role) -> Result<Self, String> {
        let mut system = System::new_all();

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

        system.refresh_all();

        // We display all disks' information:
        println!("=> disks:");
        for disk in system.disks() {
            println!("{:?}", disk);
        }

        // Network interfaces name, data received and data transmitted:
        println!("=> networks:");
        for (interface_name, data) in system.networks() {
            println!(
                "{}: {}/{} B",
                interface_name,
                data.received(),
                data.transmitted()
            );
        }

        // Components temperature:
        println!("=> components:");
        for component in system.components() {
            println!("{:?}", component);
        }

        println!("=> system:");
        // RAM and swap information:
        println!("total memory: {} bytes", system.total_memory());
        println!("used memory : {} bytes", system.used_memory());
        println!("total swap  : {} bytes", system.total_swap());
        println!("used swap   : {} bytes", system.used_swap());

        // Display system information:
        println!("System Name:             {:?}", system.name());
        println!("Kernel Version:   {:?}", system.kernel_version());
        println!("OS Version:       {:?}", system.os_version());
        println!("Hostname:        {:?}", system.host_name());

        // Number of CPUs:
        println!("# of CPUs: {}", system.cpus().len());

        // CPU utilization:
        println!("=> CPU utilization:");
        for (i, cpu) in system.cpus().iter().enumerate() {
            println!("CPU {} at {}", i, cpu.cpu_usage());
        }

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            role,
            distribution_manager,
            command_processor,
            listener,
            system,
        })
    }
}
