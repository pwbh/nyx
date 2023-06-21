use std::net::TcpListener;

use uuid::Uuid;

use crate::Role;

mod record;

pub struct Partition {
    id: String,
    role: Role,
    queue: Vec<record::Record>,
    listener: TcpListener,
}

impl Partition {
    pub fn new(addr: String) -> Result<Self, String> {
        let listener = match TcpListener::bind(addr) {
            Ok(l) => l,
            Err(e) => return Err(e.to_string()),
        };

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            role: Role::Neutral,
            queue: vec![],
            listener,
        })
    }
}
