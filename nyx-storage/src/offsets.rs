#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Offsets {
    start: usize,
    end: usize,
}

impl Offsets {
    pub fn new(start: usize, end: usize) -> Result<Self, String> {
        if start >= end {
            return Err(format!(
                "Start ({}) can't be greater or equal to end ({})",
                start, end
            ));
        }

        Ok(Self { start, end })
    }

    pub fn to_bytes(&self, index: usize) -> ([u8; 8], [u8; 8], [u8; 8]) {
        let index_bytes = index.to_be_bytes();
        let start_bytes = self.start.to_be_bytes();
        let end_bytes = self.end.to_be_bytes();

        (index_bytes, start_bytes, end_bytes)
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> usize {
        self.end
    }
}
