use crate::kmerge_impl;
use crate::rayon;
use crate::steal;
const MIN_WORK_SIZE: usize = 1000;

// The slice has one more item in front you want to take
pub unsafe fn put_back_item<T>(slice: &[T]) -> &[T] {
    // we now get the head by constructing a slice that's one element larger at
    // the front
    let start = slice.as_ptr();
    let start = start.offset(-1);
    let len = slice.len() + 1;
    std::slice::from_raw_parts(start, len)
}
#[derive(Debug, PartialEq, Eq)]
pub struct MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub data: &'a mut [T], // that's where it starts and should be after it's merged
    pub buffer: &'a mut [T], // that's where it temporarily might be
    pub in_data: bool,     // true if the sorted data is in the data, false if it's buffer
}
impl<'a, T> MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub fn new(data: &'a mut [T], buffer: &'a mut [T], in_data: bool) -> MergeResult<'a, T> {
        assert_eq!(data.len(), buffer.len());
        MergeResult {
            data,
            buffer,
            in_data,
        }
    }
    pub fn location(self: &'a Self) -> &'a [T] {
        if self.in_data {
            self.data
        } else {
            self.buffer
        }
    }
    pub fn len(self: &'a Self) -> usize {
        return self.data.len();
    }

    pub fn merge(mut self: &mut Self, other: MergeResult<T>) -> &mut Self {
        assert!(self.in_data == other.in_data);
        unsafe {
            // be sure that the next block as actually after this block
            assert_eq!(
                self.data.as_ptr().offset(self.data.len() as isize),
                other.data.as_ptr()
            );
            assert_eq!(
                self.buffer.as_ptr().offset(self.buffer.len() as isize),
                other.buffer.as_ptr()
            );
        }
        let buffer = fuse_slices(self.buffer, other.buffer);
        let data = fuse_slices(self.data, other.data);
        if self.in_data {
            // TODO: this could probably by simpler
            two_merge1(self.location(), other.location(), buffer);
        } else {
            two_merge1(self.location(), other.location(), data);
        }
        self.in_data = !self.in_data;
        self.data = data;
        self.buffer = buffer;
        self
    }
}
pub fn fuse_slices<'a, 'b, 'c: 'a + 'b, T: 'c>(s1: &'a mut [T], s2: &'b mut [T]) -> &'c mut [T] {
    let ptr1 = s1.as_mut_ptr();
    unsafe {
        assert_eq!(ptr1.add(s1.len()) as *const T, s2.as_ptr());
        std::slice::from_raw_parts_mut(ptr1, s1.len() + s2.len())
    }
}

struct MergeProgress {
    left: usize,
    right: usize,
    output: usize,
    work_size: usize,
}

fn unsafe_manual_merge2<T>(progress: &mut MergeProgress, left: &[T], right: &[T], output: &mut [T])
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

pub type InData = bool;
pub fn two_merge1<T>(a: &[T], b: &[T], buffer: &mut [T])
where
    T: Ord + Sync + Send + Copy,
{
    assert_eq!(a.len() + b.len(), buffer.len());
    // let mut iter = a.iter().merge(b.iter());
    // let mut buffer = buffer.iter_mut();
    let mut progress = MergeProgress {
        left: 0,
        right: 0,
        output: 0,
        work_size: 0,
    };
    loop {
        let steal_counter = steal::get_my_steal_count();
        let work_left = buffer.len() - progress.output;
        if steal_counter == 0 || work_left < MIN_WORK_SIZE {
            // Do a part of the work
            progress.work_size = std::cmp::min(MIN_WORK_SIZE, work_left);
            unsafe_manual_merge2(&mut progress, &a, &b, buffer);
            if buffer.len() == progress.output {
                return; // finished
            }
            assert!(buffer.len() >= progress.output);
        } else {
            let (_, r) = a.split_at(progress.left);
            let a = r;
            let (_, r) = b.split_at(progress.right);
            let b = r;
            let (_, r) = buffer.split_at_mut(progress.output);
            let buffer = r;
            assert_eq!(a.len() + b.len(), buffer.len());

            fn spawn<T>(steal_counter: usize, a: &[T], b: &[T], buffer: &mut [T])
            where
                T: Ord + Send + Sync + Copy,
            {
                // Split the inputs and buffer into steal_counter subslices
                // the longer slice
                let max_slice = if a.len() > b.len() { a } else { b };

                if steal_counter == 1 || max_slice.len() < MIN_WORK_SIZE {
                    // finished splitting, let's just merge
                    rayon_logs::subgraph("merging", buffer.len(), || {
                        two_merge1(a, b, buffer);
                    });
                    return;
                }

                // we split the maximum slice an len / stealers element.
                // For the other slice, we split at the same element.
                let split = max_slice.len() / steal_counter;
                // the element to split
                let split_elem = max_slice[split];

                // find the splitting points in all splices
                let index_a = split_for_merge(a, &|a, b| a < b, &split_elem);
                let index_b = split_for_merge(b, &|a, b| a < b, &split_elem);
                let (left_a, right_a) = a.split_at(index_a);
                let (left_b, right_b) = b.split_at(index_b);

                let (b1, b2) = buffer.split_at_mut(left_a.len() + left_b.len());

                rayon_logs::join(
                    || spawn(steal_counter - 1, right_a, right_b, b2),
                    || rayon_logs::subgraph("merging", b1.len(), || two_merge1(left_a, left_b, b1)),
                );
            }
            spawn(steal_counter + 1 /* me */, a, b, buffer);

            return;
        }
    }
}

fn split_for_merge<T, F>(left: &[T], is_less: &F, elem: &T) -> usize
where
    F: Fn(&T, &T) -> bool,
{
    let mut a = 0;
    let mut b = left.len();
    while a < b {
        let m = a + (b - a) / 2;
        if is_less(elem, &left[m]) {
            b = m;
        } else {
            a = m + 1;
        }
    }
    a
}
