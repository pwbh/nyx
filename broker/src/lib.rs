use std::net::TcpStream;

use partition::Partition;
use topic::Topic;

mod partition;
mod topic;

struct Broker {
    id: String,
    topics: Vec<Topic>,
    partitions: Vec<Partition>,
    observer_stream: TcpStream,
}

impl Broker {
    pub fn new() -> Result<Self, String> {
        unimplemented!()
    }
}
