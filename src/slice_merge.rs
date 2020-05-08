use crate::merge::Task;
use crate::steal;
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
impl<T> SliceMerge<T>
where
    T: Copy + Ord,
{
    pub fn new(left: &[T], right: &[T], output: &mut [T], work_size: usize) -> SliceMerge<T> {
        assert!(left.len() + right.len() == output.len());
        unsafe {
            return SliceMerge {
                left: left.as_ptr(),
                left_end: (left.last().unwrap() as *const T).add(1),
                right: right.as_ptr(),
                right_end: (right.last().unwrap() as *const T).add(1),
                output: output.as_mut_ptr(),
                output_end: (output.last().unwrap() as *const T).add(1),
                work_size,
            };
        }
    }
    pub fn progressive_merge(&mut self, mut f: Option<&mut dyn Task>) {
        // let now = std::time::Instant::now();
        while self.work_left() != 0 {
            let steal_count = steal::get_my_steal_count();
            if self.work_left() < 32 * self.work_size || steal_count == 0 {
                self.merge();
            } else {
                let f = f.take();
                // if let Some(f) = f {
                //     println!("Taking");
                //     rayon::join(|| self.progressive_merge(None), || f.run());
                // } else {
                let mut other = self.split();
                if steal_count > 2 {
                    // divide in 4
                    let mut self_other = self.split();
                    let mut other_other = other.split();
                    rayon::join(
                        || {
                            rayon::join(
                                || {
                                    rayon::join(
                                        || {
                                            steal::reset_my_steal_count();
                                            self.progressive_merge(None)
                                        },
                                        || self_other.progressive_merge(None),
                                    )
                                },
                                || other.progressive_merge(None),
                            )
                        },
                        || other_other.progressive_merge(None),
                    );
                } else {
                    rayon::join(
                        || self.progressive_merge(f),
                        || other.progressive_merge(None),
                    );
                }
            }
        }
    }

    pub fn work_left(&self) -> usize {
        return (self.output_end as usize - self.output as usize) / mem::size_of::<T>();
    }
    pub fn split(&mut self) -> SliceMerge<T> {
        unsafe {
            // get back the slices
            let left = std::slice::from_raw_parts(
                self.left,
                (self.left_end as usize - self.left as usize) / mem::size_of::<T>(),
            );
            let right = std::slice::from_raw_parts(
                self.right,
                (self.right_end as usize - self.right as usize) / mem::size_of::<T>(),
            );
            let output = std::slice::from_raw_parts_mut(
                self.output,
                (self.output_end as usize - self.output as usize) / mem::size_of::<T>(),
            );
            let (left_left, left_right) = left.split_at(left.len() / 2);

            // split the right side at the same element than the left side
            let i = SliceMerge::split_for_merge(right, &*left_right.as_ptr());
            let (right_left, right_right) = right.split_at(i);
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
    fn split_for_merge(left: &[T], elem: &T) -> usize {
        let mut a = 0;
        let mut b = left.len();
        while a < b {
            let m = a + (b - a) / 2;
            if elem < &left[m] {
                b = m;
            } else {
                a = m + 1;
            }
        }
        a
    }

    pub fn merge(&mut self) {
        assert!(self.output as *const T != self.output_end);
        unsafe {
            let left_work_end = std::cmp::min(self.left_end, self.left.add(self.work_size));
            let right_work_end = std::cmp::min(self.right_end, self.right.add(self.work_size));
            let mut left: *const T = self.left;
            let mut right: *const T = self.right;
            let mut output: *mut T = self.output;
            while left < left_work_end && right < right_work_end {
                let to_copy = if *left < *right {
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
                return;
            };
            // assert!(self.left < self.left_end || self.right < self.right_end);
            let len = (self.right_end as usize - self.right as usize) / mem::size_of::<T>();
            ptr::copy_nonoverlapping(self.right, self.output, len);
            let len = (self.left_end as usize - self.left as usize) / mem::size_of::<T>();
            ptr::copy_nonoverlapping(self.left, self.output, len);
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
}
