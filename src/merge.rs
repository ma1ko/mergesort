use crate::rayon;
use crate::slice_merge;
use crate::steal;
// use std::sync::atomic::AtomicUsize;

lazy_static! {
    static ref MIN_MERGE_SIZE: usize = std::env::var("MERGESIZE")
        .map(|x| x.parse::<usize>().unwrap())
        .unwrap_or(16);
    static ref SPLIT_THRESHOLD: usize = 32 * 4 * *MIN_MERGE_SIZE;
    // pub static ref MERGE_SPEEDS: Vec<(AtomicUsize, AtomicUsize)> =
        // (0..num_cpus::get()).map(|_| Default::default()).collect();
}
pub type RunTask = dyn FnMut() -> () + Sync + Send;
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
    // pub fn tmp(self: &'a Self) -> &'a [T] {
    //     if self.in_data {
    //         self.buffer
    //     } else {
    //         self.data
    //     }
    // }

    pub fn len(self: &Self) -> usize {
        return self.data.len();
    }

    pub fn merge(mut self: &mut Self, other: MergeResult<T>, f: Option<&mut dyn Task>) {
        // assert_eq!(self.in_data, other.in_data);
        assert!(self.data.len() == other.data.len());
        let mut buffer = fuse_slices(self.buffer, other.buffer);
        let mut data = fuse_slices(self.data, other.data);

        // if buffer.len() < *SPLIT_THRESHOLD {
        let (src, mut dst) = if self.in_data {
            (&mut data, &mut buffer)
        } else {
            (&mut buffer, &mut data)
        };
        // let ptr = buffer.as_mut_ptr();
        let (left_data, right_data) = &mut src.split_at_mut(self.data.len());
        // let (left_buffer, right_buffer) = &mut buffer.split_at_mut(self.data.len());
        // left_buffer.copy_from_slice(left_data);
        let x = Merge {
            left: left_data, // left_buffer,
            right: right_data,
            real_right: None,
            // right: unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr(), self.data.len()) },
            //
            //&mut dst.split_at_mut(self.data.len()).1,
            to: &mut dst,
            progress: Default::default(),
            f,
        };
        let mut merge = slice_merge::SliceMerge::new(x.left, x.right, x.to, *MIN_MERGE_SIZE);

        merge.progressive_merge();
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
fn cut_off_right_mut<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = left;
    right
}
fn cut_off_right<'a, T>(s: &mut &'a [T], mid: usize) -> &'a [T] {
    let tmp: &'a [T] = ::std::mem::replace(&mut *s, &[]);
    let (left, right) = tmp.split_at(mid);
    *s = left;
    right
}
fn cut_off_left<'a, T>(s: &mut &'a [T], mid: usize) -> &'a [T] {
    let tmp: &'a [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at(mid);
    *s = right;
    left
}
fn cut_off_left_mut<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = right;
    left
}

pub struct Merge<'a, 'b, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub left: &'a mut [T],
    pub right: &'a mut [T],
    pub real_right: Option<&'a mut [T]>,
    pub to: &'a mut [T],
    pub progress: MergeProgress,
    pub f: Option<&'b mut dyn Task>,
}

impl<'a, 'b, T> Merge<'a, 'b, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    // pub fn two_merge(&mut self) {
    //     assert_eq!(self.left.len() + self.right.len(), self.to.len());
    //     assert_ne!(self.left.as_ptr(), self.to.as_ptr());
    //     let x = self.left.as_mut_ptr();
    //     unsafe {
    //         // std::ptr::copy_nonoverlapping(
    //         //     self.left.as_ptr(),
    //         //     self.to.as_mut_ptr(),
    //         //     self.left.len(),
    //         // );
    //         // let left = std::slice::from_raw_parts_mut(self.to.as_mut_ptr(), self.left.len());
    //         // assert_ne!(left.as_ptr(), self.left.as_ptr());
    //         slice_merge::merge(
    //             &mut self.left,
    //             &mut self.right,
    //             self.to.as_mut_ptr(),
    //             2usize.pow(20),
    //         );
    //     }
    //     // assert_ne!(x, self.left.as_mut_ptr());
    //     // unsafe_manual_merge2_orig(&self.left, &self.right, &mut self.to);
    //     // let len = self.left.len();
    //     // self.right.copy_from_slice(&self.left[0..len / 2]);
    //     // unsafe {
    //     //     let other = std::slice::from_raw_parts(self.left.as_ptr().add(len / 2), len / 2);
    //     //     Merge::unsafe_manual_merge2_orig(&self.right, other, self.left);
    //     // }
    // }

    pub fn _progressive_merge(mut merge: &mut slice_merge::SliceMerge<T>) {
        // let now = std::time::Instant::now();
        // assert_eq!(self.left.len() + self.right.len(), self.to.len());
        // self.progress = Default::default();
        // let mut progress = &mut self.progress;
        //
        loop {
            if merge.work_left() == 0 {
                return;
            };
            if steal::get_my_steal_count() == 0 {
                // || work_left < *SPLIT_THRESHOLD {
                merge.merge();
            // Do a part of the work
            // merge.work_size = std::cmp::min(*MIN_MERGE_SIZE, merge.work_left);
            // unsafe_manual_merge2(&mut progress, &self.left, &self.right, self.to);

            // let i = ::rayon::current_thread_index().unwrap();
            // use std::sync::atomic::Ordering::Relaxed;
            // MERGE_SPEEDS[i].0.fetch_add(self.to.len(), Relaxed);
            // MERGE_SPEEDS[i]
            //     .1
            //     .fetch_add(now.elapsed().as_micros() as usize, Relaxed);
            // assert!(self.to.len() >= merge.output);
            } else {
                println!("Test");
                let mut other = merge.split();
                merge.merge();
                // rayon::join(
                //     || Merge::progressive_merge(&mut merge),
                //     || Merge::progressive_merge(&mut other),
                // );
                return;
                // // we got stolen, split off the part that is already finished
                // let r = cut_off_left_mut(&mut self.left, progress.left);
                // let a = r;
                // let r = cut_off_left_mut(&mut self.right, progress.right);
                // let b = r;
                // let r = cut_off_left_mut(&mut self.to, progress.output);
                // let buffer = r;
                // assert_eq!(a.len() + b.len(), buffer.len());

                // //  try split the mergesort. For borrowing, we need to take the sort callback
                // let mut f = std::mem::replace(&mut self.f, None);
                // if let Some(f) = &mut f {
                //     rayon::join(|| self.spawn(steal_counter), || f.run());
                //     return;
                // };
                // let _ = std::mem::replace(&mut self.f, f);

                // // didn't work, just split the merge
                // self.spawn(steal_counter + 1 /* me */);
                // return;
            }
        }
    }
    fn spawn(&mut self, steal_counter: usize) {
        if steal_counter == 1 || std::cmp::max(self.left.len(), self.right.len()) < *MIN_MERGE_SIZE
        {
            //recursive base case
            // finished splitting, let's just merge
            // rayon::subgraph("merging", self.to.len(), || {
            //     self.two_merge();
            // });
            return;
        }
        if let Some(mut real_right) = self.real_right.take() {
            let len = real_right.len();
            let _ = cut_off_left_mut(&mut real_right, len - self.right.len());
            assert_eq!(self.right.len(), real_right.len());
            real_right.copy_from_slice(self.right);
            self.right = real_right;
        }
        // Split the inputs and buffer into steal_counter subslices
        let left = std::mem::replace(&mut self.left, &mut []);
        let right = std::mem::replace(&mut self.right, &mut []);
        let max_slice = std::cmp::max(left.len(), right.len());

        // we split the maximum slice an len / stealers element.
        // For the other slice, we split at the same element.
        let split = max_slice / steal_counter;
        // the element to split
        let split_elem = if left.len() > right.len() {
            left[split]
        } else {
            right[split]
        };
        // find the splitting points in all splices
        let index_left = split_for_merge(left, &|a, b| a < b, &split_elem);
        let index_right = split_for_merge(right, &|a, b| a < b, &split_elem);
        let (me_left, mut other_left) = left.split_at_mut(index_left);
        let (me_right, mut other_right) = right.split_at_mut(index_right);
        // let (me_real_right, other_real_right) = self
        //     .real_right
        //     .take()
        //     .map(|x| {
        //         let x = x.split_at_mut(index_right);
        //         (Some(x.0), Some(x.1))
        //     })
        //     .unwrap_or((None, None));
        /*        [ for me    | other task]
         * left:  [me_left  | other_left]
         * right: [me_right | other_right]
         * to:    [me_to    | other_to]
         */

        let other_to = cut_off_right_mut(&mut self.to, me_left.len() + me_right.len());
        let mut other = Merge {
            left: &mut other_left,
            right: &mut other_right,
            real_right: None,
            to: other_to,
            progress: Default::default(),
            f: None,
        };
        // self.real_right = me_real_right;
        self.left = me_left;
        self.right = me_right;
        assert_eq!(self.left.len() + self.right.len(), self.to.len());
        assert_eq!(other.left.len() + other.right.len(), other.to.len());

        // rayon::join(
        //     || self.spawn(steal_counter - 1),
        //     || rayon::subgraph("merging", other.to.len(), || other.two_merge()),
        // );
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
