use std::{fmt::Debug, path::PathBuf};

use heed::{types::OwnedType, Database, Env, EnvOpenOptions};
use shared_structures::DirManager;

use super::record::Record;

struct DB {
    env: Env,
    database: Database<OwnedType<u128>, Record>,
}

pub struct PartitionDB {
    pub offest: u128,
    db: Option<DB>,
}

impl PartitionDB {
    pub fn with_dir(replica_id: &str, custom_dir: Option<&PathBuf>) -> Result<Self, String> {
        let storage_dir_path = if let Some(custom_dir) = custom_dir {
            let mut dir = custom_dir.clone();
            dir.push("/storage");
            dir
        } else {
            "/storage".into()
        };
        let storage_dir = DirManager::with_dir(Some(&storage_dir_path));
        let db_file_name = format!("{}.mdb", replica_id);
        let db_file_path = storage_dir
            .create(&db_file_name)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        let env = EnvOpenOptions::new()
            .open(db_file_path)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        let database: Database<OwnedType<u128>, Record> = env
            .create_database(None)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        Ok(Self {
            offest: 0,
            db: Some(DB { env, database }),
        })
    }
}

impl Debug for PartitionDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Offset: {}", self.offest)
    }
}

impl Default for PartitionDB {
    fn default() -> Self {
        Self {
            offest: 0,
            db: None,
        }
    }
}
