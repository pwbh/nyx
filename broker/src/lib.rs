use std::{fs, io::Write, net::TcpStream, path::PathBuf};

use shared_structures::{Broadcast, Message};
use uuid::Uuid;

mod message_handler;
mod partition;

pub use message_handler::MessageHandler;
pub use partition::Partition;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    id: String,
    partitions: Vec<Partition>,
}

#[derive(Debug)]
pub struct Broker {
    pub metadata: Metadata,
    pub stream: TcpStream,
    custom_dir: Option<PathBuf>,
}

impl Broker {
    /// Broker will automatically initiate a handshake with the Observer
    pub fn new(stream: TcpStream, name: Option<&String>) -> Result<Self, String> {
        let custom_dir: Option<PathBuf> = name.map(|f| f.into());

        let mut broker = match try_get_metadata(custom_dir.as_ref()) {
            Ok(metadata) => Self {
                stream,
                metadata,
                custom_dir,
            },
            Err(_e) => {
                let id = Uuid::new_v4().to_string();

                let metadata = Metadata {
                    id,
                    partitions: vec![],
                };

                let broker = Self {
                    metadata,
                    stream,
                    custom_dir,
                };

                broker.save_metadata_file()?;

                broker
            }
        };

        broker.handshake()?;

        Ok(broker)
    }

    fn handshake(&mut self) -> Result<(), String> {
        Broadcast::to(
            &mut self.stream,
            &Message::BrokerWantsToConnect {
                id: self.metadata.id.clone(),
            },
        )
    }

    fn save_metadata_file(&self) -> Result<(), String> {
        save_metadata_file(&self.metadata, self.custom_dir.as_ref())
    }

    fn create_partition(&mut self, partition: Partition) -> Result<(), String> {
        self.metadata.partitions.push(partition);
        self.save_metadata_file()
    }
}

fn save_metadata_file(metadata: &Metadata, custom_dir: Option<&PathBuf>) -> Result<(), String> {
    let nyx_dir = get_metadata_directory(custom_dir)?;
    let filepath = get_metadata_filepath(custom_dir)?;
    fs::create_dir_all(nyx_dir).map_err(|e| e.to_string())?;
    let mut file = std::fs::File::create(filepath).map_err(|e| e.to_string())?;
    let payload = serde_json::to_string(metadata).map_err(|e| e.to_string())?;
    file.write(payload.as_bytes()).map_err(|e| e.to_string())?;
    Ok(())
}

fn try_get_metadata(custom_dir: Option<&PathBuf>) -> Result<Metadata, String> {
    let filepath = get_metadata_filepath(custom_dir)?;
    let content = fs::read_to_string(filepath).map_err(|e| e.to_string())?;
    serde_json::from_str::<Metadata>(&content).map_err(|e| e.to_string())
}

fn get_metadata_filepath(custom_dir: Option<&PathBuf>) -> Result<PathBuf, String> {
    let dir = get_metadata_directory(custom_dir)?;
    let dir_str = dir
        .to_str()
        .ok_or("Not valid UTF-8 path is passed.".to_string())?;

    let filepath = format!("{}/metadata.json", dir_str);
    Ok(filepath.into())
}

fn get_metadata_directory(custom_dir: Option<&PathBuf>) -> Result<PathBuf, String> {
    let final_path = if let Some(custom_dir) = custom_dir {
        let dist = custom_dir
            .clone()
            .to_str()
            .ok_or("Invalid format provided for the directory")?
            .to_string();
        format!("nyx/{}", dist)
    } else {
        "nyx".to_string()
    };

    let mut final_dir: Option<PathBuf> = None;
    // Unix-based machines
    if let Ok(home_dir) = std::env::var("HOME") {
        let config_dir = format!("{}/.config/{}", home_dir, final_path);
        final_dir = Some(config_dir.into());
    }
    // Windows based machines
    else if let Ok(user_profile) = std::env::var("USERPROFILE") {
        let config_dir = format!(r"{}/AppData/Roaming/{}", user_profile, final_path);
        final_dir = Some(config_dir.into());
    }

    final_dir.ok_or("Couldn't get the systems home directory. Please setup a HOME env variable and pass your system's home directory there.".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_nyx_dir_with_metadata(custom_dir: &PathBuf) {
        save_metadata_file(
            &Metadata {
                id: "some_mocked_id".to_string(),
                partitions: vec![],
            },
            Some(custom_dir),
        )
        .unwrap();
    }

    fn cleanup_nyx_storage(custom_dir: &PathBuf) {
        let nyx_dir = get_metadata_directory(Some(custom_dir)).unwrap();
        fs::remove_dir_all(nyx_dir).unwrap();
    }

    #[test]
    fn get_metadata_directory_returns_dir_as_expected() {
        let dir = get_metadata_directory(None).unwrap();
        assert!(dir.to_str().unwrap().contains("nyx"));
    }

    #[test]
    fn get_metadata_filepath_returns_filepath_as_expected() {
        let filepath = get_metadata_filepath(None).unwrap();
        assert!(filepath.to_str().unwrap().contains("nyx/metadata.json"));
    }

    #[test]
    fn save_metadata_file_saves_file_to_designated_location() {
        let custom_dir: PathBuf = "save_metadata_file_saves_file_to_designated_location".into();
        save_metadata_file(
            &Metadata {
                id: "broker_metadata_id".to_string(),
                partitions: vec![],
            },
            Some(&custom_dir),
        )
        .unwrap();
        let filepath = get_metadata_filepath(Some(&custom_dir)).unwrap();
        let file = fs::File::open(filepath);
        assert!(file.is_ok());
        cleanup_nyx_storage(&custom_dir);
    }

    #[test]
    fn tries_to_get_metadata_succeeds() {
        let custom_dir: PathBuf = "tries_to_get_metadata_succeeds".into();
        setup_nyx_dir_with_metadata(&custom_dir);
        let result = try_get_metadata(Some(&custom_dir));
        assert!(result.is_ok());
        cleanup_nyx_storage(&custom_dir);
    }
}
