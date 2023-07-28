use std::{
    fmt::Debug,
    fs::{self},
    io::Write,
    path::PathBuf,
};

#[derive(Debug)]
pub struct DirManager {
    custom_dir: Option<PathBuf>,
}

impl DirManager {
    pub fn new() -> Self {
        Self { custom_dir: None }
    }

    pub fn with_dir(custom_dir: Option<&PathBuf>) -> Self {
        Self {
            custom_dir: custom_dir.map(|c| c.clone()),
        }
    }

    pub fn save<'de, T: serde::Serialize + serde::Deserialize<'de>>(
        &self,
        path: &str,
        content: &T,
    ) -> Result<(), String> {
        let nyx_dir = Self::get_base_dir(self.custom_dir.as_ref())?;
        let filepath = Self::get_filepath(path, self.custom_dir.as_ref())?;
        fs::create_dir_all(nyx_dir).map_err(|e| e.to_string())?;
        let mut file = std::fs::File::create(filepath).map_err(|e| e.to_string())?;
        let payload = serde_json::to_string(content).map_err(|e| e.to_string())?;
        file.write(payload.as_bytes()).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn open<T: Debug + serde::Serialize + serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, String> {
        let filepath = Self::get_filepath(path, self.custom_dir.as_ref())?;
        let content = fs::read_to_string(filepath).map_err(|e| e.to_string())?;
        let data = serde_json::from_str::<T>(&content).map_err(|e| e.to_string())?;
        Ok(data)
    }

    fn get_filepath(path: &str, custom_path: Option<&PathBuf>) -> Result<PathBuf, String> {
        let dir = Self::get_base_dir(custom_path)?;
        let dir_str = dir
            .to_str()
            .ok_or("Not valid UTF-8 path is passed.".to_string())?;

        let filepath = format!("{}/{}", dir_str, path);
        Ok(filepath.into())
    }

    fn get_base_dir(custom_path: Option<&PathBuf>) -> Result<PathBuf, String> {
        let final_path = if let Some(custom_path) = custom_path {
            let dist = custom_path
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct LocalMetadata {
        id: String,
        partitions: Vec<String>,
    }

    fn setup_nyx_dir_with_local_metadata(custom_dir: &PathBuf) -> DirManager {
        let file_manager = DirManager::with_dir(Some(custom_dir));

        file_manager
            .save(
                "metadata.json",
                &LocalMetadata {
                    id: "some_mocked_id".to_string(),
                    partitions: vec![],
                },
            )
            .unwrap();

        file_manager
    }

    fn cleanup_nyx_dir(custom_dir: &PathBuf) {
        let nyx_dir = DirManager::get_base_dir(Some(custom_dir)).unwrap();
        fs::remove_dir_all(nyx_dir).unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn get_local_metadata_directory_returns_dir_as_expected() {
        let dir = DirManager::get_base_dir(None).unwrap();
        assert!(dir.to_str().unwrap().contains("nyx"));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn get_local_metadata_filepath_returns_filepath_as_expected() {
        let filepath = DirManager::get_filepath("metadata.json", None).unwrap();
        assert!(filepath.to_str().unwrap().contains("nyx/metadata.json"));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn save_local_metadata_file_saves_file_to_designated_dir() {
        let custom_dir: PathBuf = "save_metadata_file_saves_file_to_designated_dir".into();
        let file_manager = DirManager::with_dir(Some(&custom_dir));
        file_manager
            .save(
                "metadata.json",
                &LocalMetadata {
                    id: "broker_metadata_id".to_string(),
                    partitions: vec![],
                },
            )
            .unwrap();
        let filepath = DirManager::get_filepath("metadata.json", Some(&custom_dir)).unwrap();
        let file = fs::File::open(filepath);
        assert!(file.is_ok());
        cleanup_nyx_dir(&custom_dir);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn open_local_metadata_succeeds() {
        let custom_dir: PathBuf = "tries_to_get_metadata_succeeds".into();
        let file_manager = setup_nyx_dir_with_local_metadata(&custom_dir);
        let result = file_manager.open::<LocalMetadata>("metadata.json");
        assert!(result.is_ok());
        cleanup_nyx_dir(&custom_dir);
    }
}
