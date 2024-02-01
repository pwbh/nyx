#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(C)]
pub struct Offset {
    index: usize,
    start: usize,
    data_size: usize,
    segment_count: usize,
}

impl Offset {
    pub fn new(
        index: usize,
        start: usize,
        end: usize,
        segment_count: usize,
    ) -> Result<Self, String> {
        if start >= end {
            return Err(format!(
                "Start ({}) can't be greater or equal to end ({})",
                start, end
            ));
        }

        Ok(Self {
            index,
            start,
            data_size: end - start,
            segment_count,
        })
    }

    pub fn from(index: usize, start: usize, data_size: usize, segment_count: usize) -> Self {
        Self {
            index,
            start,
            data_size,
            segment_count,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        let offset = self as *const _ as *const [u8; std::mem::size_of::<Offset>()];
        unsafe { &(*offset) }
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn data_size(&self) -> usize {
        self.data_size
    }

    pub fn segment_count(&self) -> usize {
        self.segment_count
    }
}
