#[derive(Clone, Copy, Debug)]
pub enum Status {
    Created,
    Crashed,
    Active,
}

#[derive(Clone, Copy, Debug)]
pub enum Role {
    Follower,
    Leader,
}
