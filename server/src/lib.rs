mod topic;

pub enum Role {
    Leader,
    Follower,
    Neutral,
}

struct Server {
    id: String,
    role: Role,
    topics: Vec<topic::Topic>,
}
