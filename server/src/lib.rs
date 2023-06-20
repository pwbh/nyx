mod topic;

pub enum Role {
    Leader,
    Follower,
    Neutral,
}

struct Server {
    id: String,
    topics: Vec<topic::Topic>,
    role: Role,
}
