use std::{fmt::Debug, path::PathBuf};

use heed::{
    types::{OwnedType, SerdeJson},
    Database, Env, EnvOpenOptions,
};

use shared_structures::DirManager;

pub struct DB {
    pub length: u64,
    pub env: Env,
    pub db: Database<OwnedType<u128>, SerdeJson<String>>,
}

impl DB {
    pub fn with_dir(replica_id: &str, custom_dir: Option<&PathBuf>) -> Result<Self, String> {
        let storage_dir_path = if let Some(custom_dir) = custom_dir {
            let mut dir = custom_dir.clone();
            dir.push("storage");
            dir
        } else {
            "storage".into()
        };
        let storage_dir = DirManager::with_dir(Some(&storage_dir_path));
        let db_file_name = format!("{}.mdb", replica_id);
        let db_file_path = storage_dir
            .create(&db_file_name)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        let env = EnvOpenOptions::new()
            .open(db_file_path)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        let db: Database<OwnedType<u128>, SerdeJson<String>> = env
            .create_database(None)
            .map_err(|e| format!("PartitionDB: {}", e))?;

        let txn = env.read_txn().map_err(|e| e.to_string())?;
        let length = db.len(&txn).map_err(|e| e.to_string())?;
        txn.commit().map_err(|e| e.to_string())?;

        Ok(Self { db, env, length })
    }
}

impl Debug for DB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path = if let Some(s) = self.env.path().to_str() {
            s
        } else {
            "No path could be evaluated, not utf-8 characters."
        };

        write!(f, "env: {} | length: {}", path, self.length)
    }
}
