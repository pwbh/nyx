#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Offset {
    start: usize,
    size: usize,
}

impl Offset {
    pub fn new(start: usize, end: usize) -> Result<Self, String> {
        if start >= end {
            return Err(format!(
                "Start ({}) can't be greater or equal to end ({})",
                start, end
            ));
        }

        Ok(Self {
            start,
            size: end - start,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        let offset = self as *const _ as *const [u8; 16];
        unsafe { &(*offset) }
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
