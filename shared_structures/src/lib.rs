#[derive(Clone, Copy, Debug)]
pub enum Status {
    PendingCreation,
    Crashed,
    Active,
}

#[derive(Clone, Copy, Debug)]
pub enum Role {
    Follower,
    Leader,
}
