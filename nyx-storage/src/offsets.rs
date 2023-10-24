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

    pub fn as_bytes(&self) -> &[u8] {
        let offsets = self as *const _ as *const [u8; 16];
        unsafe { &(*offsets) }
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> usize {
        self.end
    }
}
