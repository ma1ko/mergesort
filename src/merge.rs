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
pub struct MergeResult<'a> {
    pub data: &'a mut [usize], // that's where it starts and should be after it's merged
    pub buffer: &'a mut [usize], // that's where it temporarily might be
    pub in_data: bool,         // true if the sorted data is in the data, false if it's buffer
}
impl<'a, 'b> MergeResult<'a> {
    pub fn new(data: &'a mut [usize], buffer: &'a mut [usize], in_data: bool) -> MergeResult<'a> {
        assert_eq!(data.len(), buffer.len());
        MergeResult {
            data,
            buffer,
            in_data,
        }
    }
    pub fn location(self: &'a Self) -> &'a [usize] {
        if self.in_data {
            self.data
        } else {
            self.buffer
        }
    }
    pub fn len(self: &'a Self) -> usize {
        return self.data.len();
    }

    pub fn merge(mut self: &mut Self, other: MergeResult) -> &mut Self {
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

pub type InData = bool;
pub struct Peekable<I: Iterator> {
    iter: I,
    /// Remember a peeked value, even if it was None.
    peeked: Option<Option<I::Item>>,
}
impl<I: Iterator> Peekable<I> {
    pub fn peek(&mut self) -> Option<&I::Item> {
        let iter = &mut self.iter;
        self.peeked.get_or_insert_with(|| iter.next()).as_ref()
    }
}
// Itertools::KMergeeBy
pub struct MergeBy<I, J, F>
where
    I: Iterator,
    J: Iterator<Item = I::Item>,
{
    a: Peekable<I>,
    b: Peekable<J>,
    _fused: Option<bool>,
    _cmp: F,
}
pub struct MergeLte;
pub type Merge<I, J> = MergeBy<I, J, MergeLte>;

pub fn two_merge1(a: &[usize], b: &[usize], buffer: &mut [usize]) {
    assert_eq!(a.len() + b.len(), buffer.len());
    use itertools::Itertools;
    let mut iter = a.iter().merge(b.iter());
    let mut buffer = buffer.iter_mut();
    loop {
        let steal_counter = steal::get_my_steal_count();
        if steal_counter == 0 || buffer.len() < MIN_WORK_SIZE {
            // Do a part of the work
            for _ in 0..std::cmp::min(MIN_WORK_SIZE, buffer.len()) {
                *buffer.next().unwrap() = *iter.next().unwrap();
            }
            if buffer.len() == 0 {
                return; // finished
            }
        } else {
            // Someone is trying to steal. We need to recover the slices from the merging.
            // Unsafe: If the MergeBy struct in Itertools changes, this need to be updated
            let mut iter: Merge<std::slice::Iter<usize>, std::slice::Iter<usize>> =
                unsafe { std::mem::transmute(iter) };

            let mut a = iter.a.iter.as_slice();
            let mut b = iter.b.iter.as_slice();

            // We need to check if the iterator has peeked on any not-used elements yet
            // if yes, we need to put them back in the slice
            match iter.a.peeked.take() {
                Some(Some(_)) => unsafe {
                    a = put_back_item(a);
                },
                _ => (),
            }
            match iter.b.peeked.take() {
                Some(Some(_)) => unsafe {
                    b = put_back_item(b);
                },
                _ => (),
            }
            // after the transmute we probably shouldn't use the iterator anymore
            drop(iter);
            // That's the rest of the buffer
            let buffer = buffer.into_slice();
            assert_eq!(a.len() + b.len(), buffer.len());

            fn spawn(steal_counter: usize, a: &[usize], b: &[usize], buffer: &mut [usize]) {
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
