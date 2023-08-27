pub mod command_processor;
pub mod config;
pub mod distribution_manager;

use std::{
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use command_processor::CommandProcessor;
use config::Config;
use distribution_manager::DistributionManager;
use shared_structures::{DirManager, Metadata, Role};
use sysinfo::{CpuExt, DiskExt, System, SystemExt};
use uuid::Uuid;

pub const DEV_CONFIG: &str = "dev.properties";
pub const PROD_CONFIG: &str = "prod.properties";

const DEFAULT_PORT: u16 = 2828;

const CLUSTER_FILE: &str = "cluster.json";

pub struct Observer {
    pub id: String,
    pub role: Role,
    pub listener: TcpListener,
    pub distribution_manager: Arc<Mutex<DistributionManager>>,
    pub command_processor: CommandProcessor,
    pub system: System,
    pub followers: Arc<Mutex<Vec<TcpStream>>>,
    pub dir_manager: DirManager,
}

impl Observer {
    pub fn from(config_path: &str, role: Role, name: Option<&String>) -> Result<Self, String> {
        let mut system = System::new_all();

        let port: u16 = if let Ok(port) = std::env::var("PORT") {
            port.parse::<u16>().unwrap_or(DEFAULT_PORT)
        } else {
            DEFAULT_PORT
        };

        let custom_dir: Option<PathBuf> = name.map(|f| format!("/observer/{}", f).into());

        let dir_manager = DirManager::with_dir(custom_dir.as_ref());

        let config = Config::from(config_path.into())?;

        let cluster_metadata = match dir_manager.open::<Metadata>(CLUSTER_FILE) {
            Ok(m) => m,
            Err(_) => Metadata::default(),
        };

        let distribution_manager = DistributionManager::new(config, &cluster_metadata)?;

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
            followers: Arc::new(Mutex::new(vec![])),
            dir_manager,
        })
    }

    pub fn save_cluster_state(&self) -> Result<(), String> {
        let distribution_manager_lock = self.distribution_manager.lock().unwrap();
        let metadata = distribution_manager_lock.get_cluster_metadata()?;
        self.dir_manager.save(CLUSTER_FILE, &metadata)
    }
}
