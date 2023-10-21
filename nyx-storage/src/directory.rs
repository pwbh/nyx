use std::{fmt::Debug, io::ErrorKind};

use async_std::fs::{self, File, OpenOptions};

const NYX_BASE_PATH: &str = "nyx";

#[derive(Debug, Default)]
pub struct Directory {
    base_path: String,
    title: String,
}

// Path example: nyx/title/filename

/// Directory is used to manage the internal creation and opening of the files.
impl Directory {
    pub async fn new(title: &str) -> Result<Self, String> {
        let dir = Self {
            base_path: format!("{}/{}", NYX_BASE_PATH, title),
            title: title.to_owned(),
        };

        let full_base_path = dir.get_full_base_path()?;

        match async_std::fs::create_dir_all(&full_base_path).await {
            Ok(_) => {}
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => {}
                _ => return Err(format!("Couldn't create directory: {}", e)),
            },
        };

        Ok(dir)
    }

    fn get_full_base_path(&self) -> Result<String, String> {
        let mut final_dir = Some(String::new());

        let base_path = self.base_path.clone();

        // Unix-based machines
        if let Ok(home_dir) = std::env::var("HOME") {
            let config_dir = format!("{}/.config/{}", home_dir, base_path);
            final_dir = Some(config_dir);
        }
        // Windows based machines
        else if let Ok(user_profile) = std::env::var("USERPROFILE") {
            let config_dir = format!(r"{}/AppData/Roaming/{}", user_profile, base_path);
            final_dir = Some(config_dir);
        }

        final_dir.ok_or("Couldn't get the systems home directory. Please setup a HOME env variable and pass your system's home directory there.".to_string())
    }

    fn get_file_path(&self) -> Result<String, String> {
        let full_base_path = self.get_full_base_path()?;
        Ok(format!("{}/{}.data", full_base_path, self.title))
    }

    pub async fn open(&self) -> Result<File, String> {
        let file_path = self.get_file_path()?;

        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)
            .await
        {
            Ok(file) => Ok(file),
            Err(e) => Err(format!("Directory: {}", e)),
        }
    }

    pub async fn remove(&self) -> Result<(), String> {
        let file_path = self.get_file_path()?;
        fs::remove_file(&file_path)
            .await
            .map_err(|_| format!("Failed to delete file {}", file_path))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;

    use super::*;

    async fn remove_test_file(dir: &Directory) -> Result<(), Error> {
        let filepath = dir.get_file_path().unwrap();
        async_std::fs::remove_file(filepath).await
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn open() {
        let dir = Directory::new("events-replica-1").await.unwrap();
        let open_result = dir.open().await;

        assert!(open_result.is_ok());

        let remove_test_file_result = remove_test_file(&dir).await;

        assert!(remove_test_file_result.is_ok());
    }
}
