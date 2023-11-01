use async_std::channel::Receiver;

use crate::segment::Segment;

pub struct Compactor {
    queue: Receiver<Segment>,
}

impl Compactor {
    pub async fn run(queue: Receiver<Segment>) {
        while let Ok(segment) = queue.recv().await {}
    }
}
