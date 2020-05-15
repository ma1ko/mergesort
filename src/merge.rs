use crate::slice_merge;
pub use crate::task::Task;
// use std::sync::atomic::AtomicUsize;

lazy_static! {
    static ref MIN_MERGE_SIZE: usize = std::env::var("MERGESIZE")
        .map(|x| x.parse::<usize>().unwrap())
        .unwrap_or(1024);
    static ref SPLIT_THRESHOLD: usize = 32 * *MIN_MERGE_SIZE;
    // pub static ref MERGE_SPEEDS: Vec<(AtomicUsize, AtomicUsize)> =
        // (0..num_cpus::get()).map(|_| Default::default()).collect();
}

#[derive(Debug, PartialEq, Eq)]
pub struct MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    pub data: &'a mut [T], // that's where it starts and should be after it's merged
    pub buffer: &'a mut [T], // that's where it temporarily might be
    pub offset: usize,     // index in total
}

impl<'a, T> MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    pub fn new(data: &'a mut [T], buffer: &'a mut [T], offset: usize) -> MergeResult<'a, T> {
        assert_eq!(data.len(), buffer.len());
        MergeResult {
            data,
            buffer,
            offset,
        }
    }
    pub fn len(self: &Self) -> usize {
        return self.data.len();
    }
    pub fn is_sorted(self: &Self) -> bool {
        self.data.windows(2).all(|w| w[0] <= w[1])
    }

    pub fn merge(mut self: &mut Self, other: MergeResult<T>, f: Option<&mut impl Task>) {
        // S: Sync + Send{
        // assert_eq!(self.in_data, other.in_data);
        // assert_eq!(self.data.len(), other.data.len());
        // if self.data.len() != other.data.len() {
        //     println!("Uneven merge: {} and {}", self.data.len(), other.data.len());
        // }
        let mut buffer = fuse_slices(self.buffer, other.buffer);
        let mut merge =
            slice_merge::SliceMerge::new(self.data, other.data, &mut buffer, *MIN_MERGE_SIZE);
        let data = fuse_slices(self.data, other.data);

        self.data = buffer;
        self.buffer = data;

        merge.run(f);
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
