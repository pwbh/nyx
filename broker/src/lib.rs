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
        match try_get_metadata() {
            Ok(metadata) => Ok(Self { stream, metadata }),
            Err(e) => {
                println!("Error: {}", e);
                let id = Uuid::new_v4().to_string();

                let broker = Self {
                    metadata: Metadata {
                        id,
                        partitions: vec![],
                    },
                    stream,
                };

                let dir = get_metadata_directory()?;

                fs::create_dir(dir).map_err(|e| e.to_string())?;

                Ok(broker)
            }
        }
    }

    pub fn handshake(&mut self) -> std::io::Result<usize> {
        let payload = format!("{}\n", self.metadata.id);
        self.stream.write(payload.as_bytes())
    }
}

fn try_get_metadata() -> Result<Metadata, String> {
    let dir = get_metadata_directory()?;

    unimplemented!()
}

fn get_metadata_directory() -> Result<PathBuf, String> {
    // Unix-based machines
    if let Ok(home_dir) = std::env::var("HOME") {
        let config_dir = format!("{}/.config/nyx", home_dir);
        Ok(config_dir.into())
    }
    // Windows based machines
    else if let Ok(user_profile) = std::env::var("USERPROFILE") {
        let config_dir = format!(r"{}\AppData\Roaming\nyx", user_profile);
        Ok(config_dir.into())
    } else {
        Err("Couldn't get the systems home directory. Please setup a HOME env variable and pass your system's home directory there.".to_string())
    }
}
