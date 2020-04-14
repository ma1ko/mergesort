use crate::rayon;
use crate::steal;
const MIN_WORK_SIZE: usize = 5000;

pub type RunTask = dyn FnMut() -> () + Sync + Send;
pub trait Task: Send + Sync {
    // run self *and* me, or return false if you can't
    fn run(&mut self) -> bool;
}

#[derive(Debug, PartialEq, Eq)]
pub struct MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub data: &'a mut [T], // that's where it starts and should be after it's merged
    pub buffer: &'a mut [T], // that's where it temporarily might be
    pub in_data: bool,     // true if the sorted data is in the data, false if it's buffer
    pub offset: usize,     // index in total
}

impl<'a, T> MergeResult<'a, T>
where
    T: Ord + Sync + Send + Copy,
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
            offset: offset,
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
        assert!(self.in_data == other.in_data);
        let buffer = fuse_slices(self.buffer, other.buffer);
        let data = fuse_slices(self.data, other.data);
        // if self.location().last().unwrap() <= other.location().first().unwrap() {
        //     // it's already sorted
        //     println!("Sorted"); // not sure if this actually works (probably not)
        //     return;
        // }
        let mut merge: Merge<T> = Merge {
            left: &mut self.location(),
            right: &mut other.location(),
            to: if self.in_data { buffer } else { data },
            progress: Default::default(),
            f: f,
        };
        merge.two_merge();
        self.in_data = !self.in_data;
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
fn cut_off_right<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = left;
    right
}
pub struct Merge<'a, 'b, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub left: &'a [T],
    pub right: &'a [T],
    pub to: &'a mut [T],
    pub progress: MergeProgress,
    pub f: Option<&'b mut dyn Task>,
}
// impl<'a, T> Task for Merge<'a, T>
// where
//     T: Ord + Sync + Send + Copy,
// {
//     fn run(&mut self, me: Option<&mut dyn Task>) -> bool {
//         if me.is_none() {
//             return false;
//         }
//         unimplemented!(); // TODO
//         return false;
//     }
// }
impl<'a, 'b, T> Merge<'a, 'b, T>
where
    T: Ord + Sync + Send + Copy,
{
    pub fn two_merge(&mut self) {
        assert_eq!(self.left.len() + self.right.len(), self.to.len());
        self.progress = Default::default();
        let mut progress = &mut self.progress;
        loop {
            let steal_counter = steal::get_my_steal_count();
            let work_left = self.to.len() - progress.output;
            if steal_counter == 0 || work_left < MIN_WORK_SIZE {
                // Do a part of the work
                progress.work_size = std::cmp::min(MIN_WORK_SIZE, work_left);
                unsafe_manual_merge2(&mut progress, &self.left, &self.right, self.to);
                if self.to.len() == progress.output {
                    return; // finished
                }
                assert!(self.to.len() >= progress.output);
            } else {
                let (_, r) = self.left.split_at(progress.left);
                let a = r;
                let (_, r) = self.right.split_at(progress.right);
                let b = r;
                let (_, r) = self.to.split_at_mut(progress.output);
                let buffer = r;
                assert_eq!(a.len() + b.len(), buffer.len());

                // try split the mergesort
                let mut f = std::mem::replace(&mut self.f, None);
                if let Some(f) = &mut f {
                    rayon::join(|| self.spawn(steal_counter), || f.run());
                    return;
                };

                self.spawn(steal_counter + 1 /* me */);
                return;
            }
        }
    }
    fn spawn(&mut self, steal_counter: usize) {
        // Split the inputs and buffer into steal_counter subslices
        // the longer slice
        let left = &self.left;
        let right = &self.right;
        let max_slice = if left.len() > right.len() {
            left
        } else {
            right
        };

        //recursive base case: just sort
        if steal_counter == 1 || max_slice.len() < MIN_WORK_SIZE {
            // finished splitting, let's just merge
            rayon::subgraph("merging", self.to.len(), || {
                self.two_merge();
            });
            return;
        }

        // we split the maximum slice an len / stealers element.
        // For the other slice, we split at the same element.
        let split = max_slice.len() / steal_counter;
        // the element to split
        let split_elem = max_slice[split];

        // find the splitting points in all splices
        let index_left = split_for_merge(left, &|a, b| a < b, &split_elem);
        let index_right = split_for_merge(right, &|a, b| a < b, &split_elem);
        let (me_left, other_left) = left.split_at(index_left);
        let (me_right, other_right) = right.split_at(index_right);
        /*        [ for me    | other task]
         * left:  [me_left  | other_left]
         * right: [me_right | other_right]
         * to:    [me_to    | other_to]
         */

        let other_to = cut_off_right(&mut self.to, me_left.len() + me_right.len());
        let mut other = Merge {
            left: &other_left,
            right: &other_right,
            to: other_to,
            progress: Default::default(),
            f: None,
        };
        self.left = me_left;
        self.right = me_right;

        rayon::join(
            || self.spawn(steal_counter - 1),
            || rayon::subgraph("merging", other.to.len(), || other.two_merge()),
        );
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
