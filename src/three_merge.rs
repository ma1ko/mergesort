use crate::slice_merge::SliceMerge;
use adaptive_algorithms::Task;
use std::mem;
use std::ptr;
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub struct ThreeMerge<T>
where
    T: Copy + Ord,
{
    pub left: *const T,
    pub left_end: *const T,
    pub middle: *const T,
    pub middle_end: *const T,

    pub right: *const T,
    pub right_end: *const T,
    pub output: *mut T,
    pub output_end: *const T,
    pub work_size: usize,
}
unsafe impl<T> Send for ThreeMerge<T> where T: Copy + Ord {}
// unsafe impl<T> Sync for SliceMerge<T> where T: Copy + Ord {}
impl<T> ThreeMerge<T>
where
    T: Copy + Ord,
{
    pub fn new(
        left: &[T],
        middle: &[T],
        right: &[T],
        output: &mut [T],
        work_size: usize,
    ) -> ThreeMerge<T> {
        assert!(left.len() + right.len() + middle.len() == output.len());
        unsafe {
            return ThreeMerge {
                left: left.as_ptr(),
                left_end: left.as_ptr().add(left.len()),
                middle: middle.as_ptr(),
                middle_end: middle.as_ptr().add(left.len()),
                right: right.as_ptr(),
                right_end: right.as_ptr().add(right.len()),
                output: output.as_mut_ptr(),
                output_end: output.as_ptr().add(output.len()),
                work_size,
            };
        }
    }
    pub fn work_left(&self) -> usize {
        diff(self.output, self.output_end)
    }
}

impl<T> Task for ThreeMerge<T>
where
    T: Copy + Ord + Sync + Send,
{
    fn step(&mut self) {
        // self.check();
        assert!(self.output as *const T != self.output_end);
        unsafe {
            let left_work_end = std::cmp::min(self.left_end, self.left.add(self.work_size));
            let middle_work_end = std::cmp::min(self.middle_end, self.middle.add(self.work_size));
            let right_work_end = std::cmp::min(self.right_end, self.right.add(self.work_size));
            let mut left: *const T = self.left;
            let mut middle: *const T = self.middle;
            let mut right: *const T = self.right;
            let mut output: *mut T = self.output;

            // while left < left_work_end && right < right_work_end && middle < middle_work_end {
            let mut to_copy;
            loop {
                if *left <= *middle && *left <= *right {
                    to_copy = get_and_increment(&mut left);
                    ptr::copy_nonoverlapping(to_copy, get_and_increment_mut(&mut output), 1);
                    if left == left_work_end {
                        break;
                    };
                } else {
                    if *middle <= *right {
                        to_copy = get_and_increment(&mut middle);
                        ptr::copy_nonoverlapping(to_copy, get_and_increment_mut(&mut output), 1);
                        if middle == middle_work_end {
                            break;
                        };
                    } else {
                        to_copy = get_and_increment(&mut right);
                        ptr::copy_nonoverlapping(to_copy, get_and_increment_mut(&mut output), 1);
                        if right == right_work_end {
                            break;
                        };
                    }
                };
            }
            // ptr::copy_nonoverlapping(to_copy, get_and_increment_mut(&mut output), 1);
            self.left = left;
            self.middle = middle;
            self.right = right;
            self.output = output;
            if self.left < self.left_end
                && self.right < self.right_end
                && self.middle < self.middle_end
            {
                // no side is finished yet
                return;
            };
            // one side is finished, copy over the remainder from the other side
            let left = from_raw_parts(self.left, diff(self.left, self.left_end));
            let middle = from_raw_parts(self.middle, diff(self.middle, self.middle_end));
            let right = from_raw_parts(self.right, diff(self.right, self.right_end));
            let output = from_raw_parts_mut(self.output, diff(self.output, self.output_end));
            assert_eq!(left.len() + right.len() + middle.len(), output.len());

            if self.left == self.left_end {
                SliceMerge::new(middle, right, output, self.work_size).run();
            } else if self.middle == self.middle_end {
                SliceMerge::new(left, right, output, self.work_size).run();
            } else if self.right == self.right_end {
                SliceMerge::new(left, middle, output, self.work_size).run();
            }
            self.output = self.output_end as *mut T;
            let output = from_raw_parts_mut(self.output, diff(self.output, self.output_end));
            return;

            // // assert!(self.left < self.left_end || self.right < self.right_end);
            // ptr::copy_nonoverlapping(self.right, self.output, diff(self.right, self.right_end));
            // ptr::copy_nonoverlapping(self.left, self.output, diff(self.left, self.left_end));
            // self.output = self.output_end as *mut T;

            pub unsafe fn get_and_increment_mut<T>(ptr: &mut *mut T) -> *mut T {
                let old = *ptr;
                *ptr = ptr.offset(1);
                old
            }
            pub unsafe fn get_and_increment<T>(ptr: &mut *const T) -> *const T {
                let old = *ptr;
                *ptr = ptr.offset(1);
                old
            }
        }
    }
    fn is_finished(&self) -> bool {
        return diff(self.output, self.output_end) == 0;
    }

    fn split(&mut self, mut runner: impl FnMut(&mut Vec<&mut Self>), _steal_counter: usize) {
        unsafe {
            // get back the slices
            let left = from_raw_parts(self.left, diff(self.left, self.left_end));
            let middle = from_raw_parts(self.middle, diff(self.middle, self.middle_end));
            let right = from_raw_parts(self.right, diff(self.right, self.right_end));
            let output = from_raw_parts_mut(self.output, diff(self.output, self.output_end));

            assert!(left.len() + right.len() + middle.len() == output.len());
            assert!(output.len() > 100);
            // split on side at half (we might want to split the bigger side (?)
            let (left_index, right_index) = split_for_merge(left, right, &|a, b| a < b);
            let (left_index2, middle_index) = split_for_merge(left, middle, &|a, b| a < b);
            assert_eq!(left_index, left_index2);
            let (left_left, left_right) = left.split_at(left_index);

            // split the right side at the same element than the left side
            let (right_left, right_right) = right.split_at(right_index);
            let (middle_left, middle_right) = middle.split_at(middle_index);
            let (output_left, output_right) =
                output.split_at_mut(right_left.len() + left_left.len() + middle_left.len());
            // create another merging task will all right side slices.
            let mut other = ThreeMerge {
                left: left_right.as_ptr(),
                left_end: left_right.as_ptr().add(left_right.len()),
                middle: middle_right.as_ptr(),
                middle_end: middle_right.as_ptr().add(middle_right.len()),
                right: right_right.as_ptr(),
                right_end: right_right.as_ptr().add(right_right.len()),
                output: output_right.as_mut_ptr(),
                output_end: output_right.as_ptr().add(output_right.len()),
                work_size: self.work_size,
            };
            // just merge the left-side slices here
            self.left_end = self.left.add(left_left.len());
            self.middle_end = self.middle.add(middle_left.len());
            self.right_end = self.right.add(right_left.len());
            self.output_end = self.output.add(output_left.len());
            // println!("Parallel Merge: Left: , right: ",);
            // self.check();
            // other.check();

            runner(&mut vec![self, &mut other]);
        }
    }
    fn can_split(&self) -> bool {
        return self.work_left() > self.work_size * 32;
    }
    fn fuse(&mut self, _other: &mut Self) {
        // Nothing to do here actually
    }
}
impl<T> ThreeMerge<T>
where
    T: Ord + Copy,
{
    fn check(&self) {
        assert_eq!(
            diff(self.left, self.left_end)
                + diff(self.right, self.right_end)
                + diff(self.middle, self.middle_end),
            diff(self.output, self.output_end)
        );
    }
}

// difference between two pointer (it's in  std::ptr but only on nightly)
fn diff<T>(left: *const T, right: *const T) -> usize {
    // assert!(right as usize >= left as usize);
    (right as usize - left as usize) / mem::size_of::<T>()
}

// copied from rayon: https://github.com/rayon-rs/rayon/blob/master/src/slice/mergesort.rs
/// Splits two sorted slices so that they can be merged in parallel.
///
/// Returns two indices `(a, b)` so that slices `left[..a]` and `right[..b]` come before
/// `left[a..]` and `right[b..]`.
fn split_for_merge<T, F>(left: &[T], right: &[T], is_less: &F) -> (usize, usize)
where
    F: Fn(&T, &T) -> bool,
{
    let left_len = left.len();
    let right_len = right.len();

    // if left_len >= right_len {
    let left_mid = left_len / 2;

    // Find the first element in `right` that is greater than or equal to `left[left_mid]`.
    let mut a = 0;
    let mut b = right_len;
    while a < b {
        let m = a + (b - a) / 2;
        if is_less(&right[m], &left[left_mid]) {
            a = m + 1;
        } else {
            b = m;
        }
    }

    (left_mid, a)
    /*
    } else {
        let right_mid = right_len / 2;

        // Find the first element in `left` that is greater than `right[right_mid]`.
        let mut a = 0;
        let mut b = left_len;
        while a < b {
            let m = a + (b - a) / 2;
            if is_less(&right[right_mid], &left[m]) {
                b = m;
            } else {
                a = m + 1;
            }
        }

        (a, right_mid)
    }
           */
}
