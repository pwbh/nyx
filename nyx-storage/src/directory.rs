use std::{
    fmt::Debug,
    fs::{self},
    path::PathBuf,
};

#[derive(Debug, Default)]
pub struct Directory {
    custom_dir: Option<PathBuf>,
    title: String,
}

impl Directory {
    /// Creates a managed directory in the predefined path of the root directory for Nyx.   
    pub fn new(title: &str) -> Self {
        Self {
            custom_dir: None,
            title: title.to_owned(),
        }
    }

    /// Create a managed directory in the predefined path of the root directory
    /// for Nyx at specified custom directory `custom_dir`, this manager can then perform
    /// different fs actions such as opening a file in the directory, saving the file to the directory,
    /// and creating empty files in the directory safely in the context
    /// of the directory it was created in.
    ///
    /// when creating a Directory by passing `custom_dir` the Directory will still
    /// work in the context of the Nyx foder.
    pub fn with_dir(title: &str, custom_dir: Option<&PathBuf>) -> Self {
        Self {
            custom_dir: custom_dir.cloned(),
            title: title.to_owned(),
        }
    }

    pub fn create(&self, path: &str) -> Result<PathBuf, String> {
        let project_dir = self.get_base_dir(self.custom_dir.as_ref())?;
        let project_dir_str = project_dir
            .to_str()
            .ok_or("Failed while validating UTF-8 string integrity.")?;
        let total_path = format!("{}/{}", project_dir_str, path);
        match fs::create_dir_all(&total_path) {
            Ok(_) => {}
            Err(e) => {
                println!("Directory -> create func error: {}", e)
            }
        };
        Ok(total_path.into())
    }

    fn get_filepath(&self, path: &str, custom_path: Option<&PathBuf>) -> Result<PathBuf, String> {
        let dir = self.get_base_dir(custom_path)?;
        let dir_str = dir
            .to_str()
            .ok_or("Not valid UTF-8 string has been passed.".to_string())?;

        let filepath = format!("{}/{}", dir_str, path);
        Ok(filepath.into())
    }

    pub fn get_base_dir(&self, custom_path: Option<&PathBuf>) -> Result<PathBuf, String> {
        let final_path = if let Some(custom_path) = custom_path {
            let dist = custom_path
                .clone()
                .to_str()
                .ok_or("Invalid format provided for the directory")?
                .to_string();
            format!("{}/{}", self.title, dist)
        } else {
            self.title.clone()
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

    #[test]
    #[cfg_attr(miri, ignore)]
    fn get_local_metadata_directory_returns_dir_as_expected() {
        let dir = Directory::new("nyx-storage");
        let dir_path = dir.get_base_dir(None).unwrap();
        assert!(dir_path.to_str().unwrap().contains("nyx"));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn get_local_metadata_filepath_returns_filepath_as_expected() {
        let dir = Directory::new("nyx-storage");
        let filepath = dir.get_filepath("metadata.json", None).unwrap();
        println!("{:?}", filepath);
        assert!(filepath
            .to_str()
            .unwrap()
            .contains("nyx-storage/metadata.json"));
    }
}
