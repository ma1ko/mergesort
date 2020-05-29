use crate::merge::Task;
use std::mem;
use std::ptr;

pub struct SliceMerge<T>
where
    T: Copy + Ord,
{
    pub left: *const T,
    pub left_end: *const T,
    pub right: *const T,
    pub right_end: *const T,
    pub output: *mut T,
    pub output_end: *const T,
    pub work_size: usize,
}
unsafe impl<T> Send for SliceMerge<T> where T: Copy + Ord {}
unsafe impl<T> Sync for SliceMerge<T> where T: Copy + Ord {}
impl<T> SliceMerge<T>
where
    T: Copy + Ord,
{
    pub fn new(left: &[T], right: &[T], output: &mut [T], work_size: usize) -> SliceMerge<T> {
        assert!(left.len() + right.len() == output.len());
        unsafe {
            return SliceMerge {
                left: left.as_ptr(),
                left_end: left.as_ptr().add(left.len()),
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

impl<T> Task for SliceMerge<T>
where
    T: Copy + Ord + Sync + Send,
{
    fn step(&mut self) {
        assert!(self.output as *const T != self.output_end);
        unsafe {
            let left_work_end = std::cmp::min(self.left_end, self.left.add(self.work_size));
            let right_work_end = std::cmp::min(self.right_end, self.right.add(self.work_size));
            let mut left: *const T = self.left;
            let mut right: *const T = self.right;
            let mut output: *mut T = self.output;
            while left < left_work_end && right < right_work_end {
                let to_copy = if *left <= *right {
                    get_and_increment(&mut left)
                } else {
                    get_and_increment(&mut right)
                };
                ptr::copy_nonoverlapping(to_copy, get_and_increment_mut(&mut output), 1);
            }
            self.left = left;
            self.right = right;
            self.output = output;
            if self.left < self.left_end && self.right < self.right_end {
                // no side is finished yet
                return;
            };
            // one side is finished, copy over the remainder from the other side
            assert!(self.left < self.left_end || self.right < self.right_end);
            ptr::copy_nonoverlapping(self.right, self.output, diff(self.right, self.right_end));
            ptr::copy_nonoverlapping(self.left, self.output, diff(self.left, self.left_end));
            self.output = self.output_end as *mut T;

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

    fn split(&mut self) -> Self {
        use std::slice::{from_raw_parts, from_raw_parts_mut};
        unsafe {
            // get back the slices
            let left = from_raw_parts(self.left, diff(self.left, self.left_end));
            let right = from_raw_parts(self.right, diff(self.right, self.right_end));
            let output = from_raw_parts_mut(self.output, diff(self.output, self.output_end));

            // split on side at half (we might want to split the bigger side (?)
            let (left_index, right_index) = split_for_merge(left,right, &|a,b| a < b );
            let (left_left, left_right) = left.split_at(left_index);

            // split the right side at the same element than the left side
            let (right_left, right_right) = right.split_at(right_index);
            let (output_left, output_right) =
                output.split_at_mut(right_left.len() + left_left.len());
            // create another merging task will all right side slices.
            let other = SliceMerge {
                left: left_right.as_ptr(),
                left_end: left_right.as_ptr().add(left_right.len()),
                right: right_right.as_ptr(),
                right_end: right_right.as_ptr().add(right_right.len()),
                output: output_right.as_mut_ptr(),
                output_end: output_right.as_ptr().add(output_right.len()),
                work_size: self.work_size,
            };
            // just merge the left-side slices here
            self.left_end = self.left.add(left_left.len());
            self.right_end = self.right.add(right_left.len());
            self.output_end = self.output.add(output_left.len());
            return other;
        }
    }
    fn can_split(&self) -> bool {
        return self.work_left() > self.work_size * 32
    }

    // fn fuse(&mut self, _other: Self) {
    //     // Nothing to do here?
    // }
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

    if left_len >= right_len {
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
}
