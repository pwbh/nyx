mod topic;

pub enum Role {
    Leader,
    Follower,
    Neutral,
}

struct Server {
    id: String,
    topics: Vec<topic::Topic>,
}

impl Server {
    pub fn new() -> Result<Self, String> {
        unimplemented!()
    }
}
