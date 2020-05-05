use crate::slice_merge;
// use std::sync::atomic::AtomicUsize;

lazy_static! {
    static ref MIN_MERGE_SIZE: usize = std::env::var("MERGESIZE")
        .map(|x| x.parse::<usize>().unwrap())
        .unwrap_or(1024);
    static ref SPLIT_THRESHOLD: usize = 32 * *MIN_MERGE_SIZE;
    // pub static ref MERGE_SPEEDS: Vec<(AtomicUsize, AtomicUsize)> =
        // (0..num_cpus::get()).map(|_| Default::default()).collect();
}
// pub type RunTask = dyn FnMut() -> () + Sync + Send;
pub trait Task: Send + Sync {
    // run self *and* me, or return false if you can't
    fn run(&mut self) -> bool;
}

#[derive(Debug, PartialEq, Eq)]
pub struct MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    pub data: &'a mut [T], // that's where it starts and should be after it's merged
    pub buffer: &'a mut [T], // that's where it temporarily might be
    pub in_data: bool,     // true if the sorted data is in the data, false if it's in the buffer
    pub offset: usize,     // index in total
}

impl<'a, T> MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    pub fn new(
        data: &'a mut [T],
        buffer: &'a mut [T],
        in_data: bool,
        offset: usize,
    ) -> MergeResult<'a, T> {
        assert_eq!(data.len(), buffer.len());
        MergeResult {
            data,
            buffer,
            in_data,
            offset,
        }
    }
    pub fn location(self: &'a Self) -> &'a [T] {
        if self.in_data {
            self.data
        } else {
            self.buffer
        }
    }
    pub fn len(self: &Self) -> usize {
        return self.data.len();
    }

    pub fn merge(mut self: &mut Self, other: MergeResult<T>, f: Option<&mut dyn Task>) {
        assert_eq!(self.in_data, other.in_data);
        assert_eq!(self.data.len(), other.data.len());
        let mut buffer = fuse_slices(self.buffer, other.buffer);
        let mut data = fuse_slices(self.data, other.data);

        let (src, mut dst) = if self.in_data {
            (&mut data, &mut buffer)
        } else {
            (&mut buffer, &mut data)
        };
        let (left_data, right_data) = &mut src.split_at_mut(self.data.len());
        let mut merge =
            slice_merge::SliceMerge::new(left_data, right_data, &mut dst, *MIN_MERGE_SIZE);

        merge.progressive_merge(f);
        self.in_data = !self.in_data;

        // rayon::subgraph("merging", self.data.len(), || merge.two_merge());
        self.data = data;
        self.buffer = buffer;
    }
}
pub fn fuse_slices<'a, 'b, 'c: 'a + 'b, T: 'c>(s1: &'a mut [T], s2: &'b mut [T]) -> &'c mut [T] {
    let ptr1 = s1.as_mut_ptr();
    unsafe {
        assert_eq!(ptr1.add(s1.len()) as *const T, s2.as_ptr());
        std::slice::from_raw_parts_mut(ptr1, s1.len() + s2.len())
    }
}

#[derive(Debug, Default)]
pub struct MergeProgress {
    left: usize,
    right: usize,
    output: usize,
    work_size: usize,
}

// Merge two slices, tracking progress. We do work_size items, then return
fn _unsafe_manual_merge2<T>(progress: &mut MergeProgress, left: &[T], right: &[T], output: &mut [T])
where
    T: Ord + Copy,
{
    let mut left_index = progress.left;
    let mut right_index = progress.right;
    let (_, r) = output.split_at_mut(progress.output);
    let (l, _) = r.split_at_mut(progress.work_size);
    let output = l;
    for o in output {
        unsafe {
            if left_index >= left.len() {
                *o = *right.get_unchecked(right_index);
                right_index += 1;
            } else if right_index >= right.len() {
                *o = *left.get_unchecked(left_index);
                left_index += 1;
            } else if left.get_unchecked(left_index) <= right.get_unchecked(right_index) {
                *o = *left.get_unchecked(left_index);
                left_index += 1;
            } else {
                *o = *right.get_unchecked(right_index);
                right_index += 1;
            };
        }
    }
    progress.left = left_index;
    progress.right = right_index;
    progress.output = left_index + right_index;
}
