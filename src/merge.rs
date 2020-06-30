use crate::slice_merge;
pub use adaptive_algorithms::Task;
// use std::sync::atomic::AtomicUsize;

const BLOCKSIZE : usize = 1024;


#[derive(Debug, PartialEq, Eq)]
pub struct MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub data: &'a mut [T], // that's where it starts and should be after it's merged
    pub buffer: &'a mut [T], // that's where it temporarily might be
    pub blocksize: usize,     // index in total
}

impl<'a, T> MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub fn new(data: &'a mut [T], buffer: &'a mut [T]) -> MergeResult<'a, T> {
        assert_eq!(data.len(), buffer.len());
        MergeResult {
            data,
            buffer,
            blocksize : BLOCKSIZE
        }
    }
    pub fn len(self: &Self) -> usize {
        return self.data.len();
    }
    pub fn is_sorted(self: &Self) -> bool {
        self.data.windows(2).all(|w| w[0] <= w[1])
    }

    pub fn merge(mut self: &mut Self, other: MergeResult<T>, f: Option<&mut impl Task>) {
               let mut buffer = fuse_slices(self.buffer, other.buffer);
        let mut merge =
            slice_merge::SliceMerge::new(self.data, other.data, &mut buffer, self.blocksize);
        let data = fuse_slices(self.data, other.data);

        self.data = buffer;
        self.buffer = data;

        merge.run_with(f);
    }
}

pub fn fuse_slices<'a, 'b, 'c: 'a + 'b, T: 'c>(s1: &'a mut [T], s2: &'b mut [T]) -> &'c mut [T] {
    let ptr1 = s1.as_mut_ptr();
    unsafe {
        assert_eq!(ptr1.add(s1.len()) as *const T, s2.as_ptr());
        std::slice::from_raw_parts_mut(ptr1, s1.len() + s2.len())
    }
}

