mod record;

pub struct Partition {
    id: String,
    queue: Vec<record::Record>,
}
