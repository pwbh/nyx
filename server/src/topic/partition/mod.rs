use crate::Role;

mod record;

pub struct Partition {
    id: String,
    role: Role,
    queue: Vec<record::Record>,
}
