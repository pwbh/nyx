use std::{fs, io::Write, net::TcpStream, path::PathBuf};

use partition::Partition;

use uuid::Uuid;

mod partition;
mod topic;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    id: String,
    partitions: Vec<Partition>,
}

#[derive(Debug)]
pub struct Broker {
    pub metadata: Metadata,
    pub stream: TcpStream,
}

impl Broker {
    // TODO: Need to add logic to save the broker local information about itself to a main folder on the filesystem
    // that will contain the information for the broker to use in a situtation where it crushed, or was
    // disconnected and is now reconnecting, should reconnect with the old information, including partitions etc.
    pub fn new(stream: TcpStream) -> Result<Self, String> {
        let broker = match try_get_metadata() {
            Ok(metadata) => Self { stream, metadata },
            Err(e) => {
                let id = Uuid::new_v4().to_string();

                let metadata = Metadata {
                    id,
                    partitions: vec![],
                };

                let broker = Self { metadata, stream };

                save_metadata_file(&broker.metadata)?;

                broker
            }
        };

        Ok(broker)
    }

    pub fn handshake(&mut self) -> std::io::Result<usize> {
        let payload = format!("{}\n", self.metadata.id);
        self.stream.write(payload.as_bytes())
    }
}

fn try_get_metadata() -> Result<Metadata, String> {
    let filepath = get_metadata_filepath()?;
    let content = fs::read_to_string(filepath).map_err(|e| e.to_string())?;
    serde_json::from_str::<Metadata>(&content).map_err(|e| e.to_string())
}

fn save_metadata_file(metadata: &Metadata) -> Result<(), String> {
    let nyx_dir = get_metadata_directory()?;
    let filepath = get_metadata_filepath()?;
    fs::create_dir_all(&nyx_dir).map_err(|e| e.to_string())?;
    let mut file = std::fs::File::create(&filepath).map_err(|e| e.to_string())?;
    let payload = serde_json::to_string(metadata).map_err(|e| e.to_string())?;
    file.write(payload.as_bytes()).map_err(|e| e.to_string())?;
    Ok(())
}

fn get_metadata_filepath() -> Result<PathBuf, String> {
    let dir = get_metadata_directory()?;
    let dir_str = dir
        .to_str()
        .ok_or("Not valid UTF-8 path is passed.".to_string())?;

    let filepath = format!("{}/metadata.json", dir_str);
    Ok(filepath.into())
}

fn get_metadata_directory() -> Result<PathBuf, String> {
    // Unix-based machines
    if let Ok(home_dir) = std::env::var("HOME") {
        let config_dir = format!("{}/.config/nyx", home_dir);
        Ok(config_dir.into())
    }
    // Windows based machines
    else if let Ok(user_profile) = std::env::var("USERPROFILE") {
        let config_dir = format!(r"{}/AppData/Roaming/nyx", user_profile);
        Ok(config_dir.into())
    } else {
        Err("Couldn't get the systems home directory. Please setup a HOME env variable and pass your system's home directory there.".to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn cleanup_nyx_storage() -> Result<(), String> {
        let nyx_dir = get_metadata_directory()?;
        fs::remove_dir_all(nyx_dir).map_err(|e| e.to_string())
    }

    #[test]
    fn get_metadata_directory_returns_dir_as_expected() {
        let dir = get_metadata_directory().unwrap();
        assert!(dir.to_str().unwrap().contains("nyx"));
    }

    #[test]
    fn get_metadata_filepath_returns_filepath_as_expected() {
        let filepath = get_metadata_filepath().unwrap();
        assert!(filepath.to_str().unwrap().contains("nyx/metadata.json"));
    }

    #[test]
    fn save_metadata_file_saves_file_to_designated_location() {
        save_metadata_file(&Metadata {
            id: "broker_metadata_id".to_string(),
            partitions: vec![],
        })
        .unwrap();
        let filepath = get_metadata_filepath().unwrap();
        let file = fs::File::open(filepath);
        assert!(file.is_ok());

        cleanup_nyx_storage().unwrap();
    }
}
