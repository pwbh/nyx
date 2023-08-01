use std::path::PathBuf;

use heed::{types::OwnedType, Database, EnvOpenOptions};
use shared_structures::DirManager;

use super::record::Record;

struct PartitionDB {
    pub offest: u128,
    // Some env file
    db: Database<OwnedType<u128>, Record>,
}

impl PartitionDB {
    pub fn with_dir(replica_id: &str, custom_dir: &PathBuf) -> Result<Self, String> {
        let mut storage_dir_path = custom_dir.clone();
        storage_dir_path.push("/storage");
        let storage_dir = DirManager::with_dir(Some(&storage_dir_path));
        let db_file_name = format!("{}.mdb", replica_id);
        let db_file_path = storage_dir
            .create(&db_file_name)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        let env = EnvOpenOptions::new()
            .open(db_file_path)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        let db: Database<OwnedType<u128>, Record> = env
            .create_database(None)
            .map_err(|e| format!("PartitionDB: {}", e))?;
        Ok(Self { db, offest: 0 })
    }
}
