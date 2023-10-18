use std::{fmt::Debug, io::ErrorKind};

use async_std::fs::File;

const NYX_BASE_PATH: &'static str = "nyx";

#[derive(Debug, Default)]
pub struct Directory {
    base_path: String,
}

// Path example: nyx/title/filename

/// Directory is used to manage the internal creation and opening of the files.
impl Directory {
    pub async fn new(title: &str) -> Result<Self, String> {
        let dir = Self {
            base_path: format!("{}/{}", NYX_BASE_PATH, title),
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

    pub fn with_dir(title: &str, custom_path: Option<&str>) -> Self {
        let base_path = if let Some(custom_path) = custom_path {
            format!("{}/{}", title, custom_path)
        } else {
            title.to_owned()
        };

        Self { base_path }
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

    fn get_file_path(&self, filename: &str) -> Result<String, String> {
        let full_base_path = self.get_full_base_path()?;
        Ok(format!("{}/{}", full_base_path, filename))
    }

    pub async fn open(&self, filename: &str) -> Result<File, String> {
        let file_path = self.get_file_path(filename)?;
        File::open(file_path)
            .await
            .map_err(|e| format!("Directory: {}", e))
    }

    pub async fn create(&self, filename: &str) -> Result<File, String> {
        let file_path = self.get_file_path(filename)?;
        File::create(file_path)
            .await
            .map_err(|e| format!("Directory: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;

    use super::*;

    async fn remove_test_file(dir: &Directory, filename: &str) -> Result<(), Error> {
        let filepath = dir.get_file_path(filename).unwrap();
        async_std::fs::remove_file(filepath).await
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn create() {
        let dir = Directory::new("events-replica-1").await.unwrap();
        let create_file_result = dir.create("topic_name_1.data").await;

        assert!(create_file_result.is_ok());

        let remove_test_file_result = remove_test_file(&dir, "topic_name_1.data").await;

        assert!(remove_test_file_result.is_ok());
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn open_when_file_not_exists_should_error() {
        let dir = Directory::new("events-replica-1").await.unwrap();
        let file_result = dir.open("topic_doesnt_exist.data").await;

        assert!(file_result.is_err());
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn open_when_file_exists_should_ok() {
        let dir = Directory::new("events-replica-1").await.unwrap();
        let create_file_result = dir.create("topic_name.data").await;
        let open_file_result = dir.open("topic_name.data").await;

        assert!(create_file_result.is_ok());
        assert!(open_file_result.is_ok());

        let remove_test_file_result = remove_test_file(&dir, "topic_name.data").await;

        assert!(remove_test_file_result.is_ok());
    }
}
