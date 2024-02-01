use std::{
    fmt::Debug,
    io::{Error, ErrorKind},
};

use async_std::{
    fs::{self, File, OpenOptions},
    io,
};

const NYX_BASE_PATH: &str = "nyx";

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DataType {
    Partition,
    Indices,
}

#[derive(Debug, Default, Clone)]
pub struct Directory {
    base_path: String,
    title: String,
}

// Path example: nyx/title/filename

/// Directory is used to manage the internal creation and opening of the files.
impl Directory {
    pub async fn new(title: &str) -> io::Result<Self> {
        let dir = Self {
            base_path: format!("{}/{}", NYX_BASE_PATH, title),
            title: title.to_owned(),
        };

        let full_base_path = dir.get_base_path()?;

        match async_std::fs::create_dir_all(&full_base_path).await {
            Ok(_) => {}
            Err(e) => match e.kind() {
                ErrorKind::AlreadyExists => {}
                e => return Err(Error::new(e, format!("Couldn't create directory: {}", e))),
            },
        };

        Ok(dir)
    }

    pub fn get_file_path(&self, datatype: DataType, count: usize) -> io::Result<String> {
        let base_path = self.get_base_path()?;

        match datatype {
            DataType::Partition => Ok(format!("{}/{}_seg_{}.data", base_path, self.title, count)),
            DataType::Indices => Ok(format!("{}/{}_seg_{}.index", base_path, self.title, count)),
        }
    }

    pub fn get_base_path(&self) -> io::Result<String> {
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

        final_dir.ok_or(Error::new(ErrorKind::NotFound, "Couldn't get the systems home directory. Please setup a HOME env variable and pass your system's home directory there.".to_string()))
    }

    pub async fn open_read(&self, datatype: DataType, count: usize) -> io::Result<File> {
        let path = self.get_file_path(datatype, count)?;

        OpenOptions::new().read(true).open(path).await
    }

    pub async fn open_write(&self, datatype: DataType, count: usize) -> io::Result<File> {
        let path = self
            .get_file_path(datatype, count)
            .map_err(|e| Error::new(ErrorKind::NotFound, e))?;

        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .await
    }

    pub async fn open_read_write(&self, datatype: DataType, count: usize) -> io::Result<File> {
        let path = self
            .get_file_path(datatype, count)
            .map_err(|e| Error::new(ErrorKind::NotFound, e))?;

        OpenOptions::new().read(true).append(true).open(path).await
    }

    pub async fn open_read_write_create(
        &self,
        datatype: DataType,
        count: usize,
    ) -> io::Result<File> {
        let path = self
            .get_file_path(datatype, count)
            .map_err(|e| Error::new(ErrorKind::NotFound, e))?;

        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)
            .await
    }

    pub async fn delete_file(&self, datatype: DataType, count: usize) -> io::Result<()> {
        let path = self.get_file_path(datatype, count)?;
        fs::remove_file(&path).await
    }

    pub async fn delete_all(&self) -> io::Result<()> {
        let path = self.get_base_path()?;
        fs::remove_dir_all(&path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn open_read_and_delete() {
        let dir = Directory::new("events-replica-1").await.unwrap();

        // Opening non-existing file is not possible in read-mode only
        let open_partition_result = dir.open_read(DataType::Partition, 0).await;

        assert!(open_partition_result.is_err());

        // Opening non-existing file is not possible in read-mode only
        let open_indices_result = dir.open_read(DataType::Indices, 0).await;

        assert!(open_indices_result.is_err());
    }

    #[async_std::test]
    #[cfg_attr(miri, ignore)]
    async fn open_write_and_delete() {
        let dir = Directory::new("events-replica-2").await.unwrap();
        let open_partition_result = dir.open_write(DataType::Partition, 0).await;

        assert!(open_partition_result.is_ok());

        let open_indices_result = dir.open_write(DataType::Indices, 0).await;

        assert!(open_indices_result.is_ok());

        let delete_partition_result = dir.delete_file(DataType::Partition, 0).await;

        assert!(delete_partition_result.is_ok());

        let delete_indices_result = dir.delete_file(DataType::Indices, 0).await;

        assert!(delete_indices_result.is_ok());
    }
}
