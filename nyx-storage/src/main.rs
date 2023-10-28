use std::{error::Error, time::Instant};

use nyx_storage::Storage;

#[async_std::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let now = Instant::now();

    let mut simulated_partition_storage =
        Storage::new("simulated_partition_storage_1", 25_000, false).await?;

    let data =
   vec![
        b"hello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello world.";
        500_000
    ];

    for message in data {
        simulated_partition_storage.set(message).await?;
    }

    let elapsed = now.elapsed();

    println!("Done in: {:.2?}", elapsed);

    Ok(())
}
