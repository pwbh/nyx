use std::io;

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

    pub fn writable_bytes<'a>(&'a self, index: usize) -> io::Result<(&'a [u8], &'a [u8])> {
        let index_bytes = &index as *const _ as *const [u8; std::mem::size_of::<usize>()];
        let offsets_bytes = self as *const _ as *const [u8; std::mem::size_of::<Offsets>()];

        Ok(unsafe { (&*index_bytes, &*offsets_bytes) })
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn end(&self) -> usize {
        self.end
    }
}
