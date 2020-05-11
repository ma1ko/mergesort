#[macro_use]
extern crate lazy_static;
use crossbeam_utils as crossbeam;
pub mod merge;
pub mod rayon;
mod slice_merge;
pub mod steal;
pub mod task;

use crate::task::Task;
lazy_static! {
    static ref MIN_BLOCK_SIZE: usize = std::env::var("BLOCKSIZE")
        .map(|x| x.parse::<usize>().unwrap())
        .unwrap_or(2usize.pow(10));
    static ref MIN_SPLIT_SIZE: usize = 32 * *MIN_BLOCK_SIZE;
}
pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        .map(|x: usize| x % 1_000_000)
        .collect();

    let checksum: usize = v.iter().sum();
    println!("Finished generating");

    let pool = rayon::get_thread_pool();
    #[cfg(feature = "logs")]
    {
        let (_, log) = pool.logging_install(|| mergesort(&mut v));
        println!("Saving log");
        log.save("test").expect("failed saving log");
        println!("Saving svg");
        log.save_svg("test.svg").expect("failed saving svg");
    }
    #[cfg(not(feature = "logs"))]
    let _ = pool.install(|| mergesort(&mut v));
    assert_eq!(checksum, v.iter().sum::<usize>(), "failed merging");
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    println!("Success!");
    Ok(())
}

#[test]
pub fn test() -> Result<(), Box<dyn std::error::Error>> {
    main()
}

pub fn mergesort<T>(data: &mut [T])
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    let mut tmp_slice: Vec<T> = Vec::with_capacity(data.len());
    unsafe { tmp_slice.set_len(data.len()) }
    let data_ptr = data.as_mut_ptr();
    let len = data.len();
    let mut mergesort = Mergesort {
        data,
        to: &mut tmp_slice,
        pieces: Vec::new(),
        offset: 0,
    };
    mergesort.run(None);
    // use std::sync::atomic::Ordering;
    // for (x, t) in &*merge::MERGE_SPEEDS {
    //        let (x, t) = (x.load(Ordering::Relaxed), t.load(Ordering::Relaxed));
    //        if t == 0 {
    //            continue;
    //        }
    //        println!("{}", x / t);
    //    }

    // println!("Result: {:?}", mergesort.pieces_len());

    assert!(
        mergesort.pieces.len() == 1,
        format!("{:?}", mergesort.pieces_len())
    );

    assert!(mergesort.data.windows(2).all(|w| w[0] <= w[1]));
    // we need to check where the output landed, it's either in the original data or in the
    // buffer. If it's in the buffer, we need to copy it over
    if data_ptr != mergesort.pieces[0].data.as_mut_ptr() {
        rayon::subgraph("merging", tmp_slice.len(), || unsafe {
            std::ptr::copy_nonoverlapping(tmp_slice.as_ptr(), data_ptr, len);
        });
    };
    // keep the buffer size to 0 so it doesn't deallocate anything
    // see : https://doc.rust-lang.org/src/alloc/slice.rs.html#966
    unsafe {
        tmp_slice.set_len(0);
    }
}
// from https://stackoverflow.com/questions/42162151/rust-error-e0495-using-split-at-mut-in-a-closure
fn cut_off_left<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = right;
    left
}
fn cut_off_right<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = left;
    right
}

struct Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    data: &'a mut [T],
    to: &'a mut [T],
    pieces: Vec<merge::MergeResult<'a, T>>,
    offset: usize,
}
impl<'a, T> Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    fn pieces_len(&self) -> Vec<usize> {
        // mostly for debugging
        self.pieces.iter().map(|x| x.len()).collect()
    }
    fn merge(&mut self)
    where
        T: Ord + Sync + Send + Copy,
    {
        while self.pieces.len() >= 2 {
            // to merge we need at least two parts, they need to be same size
            let len = self.pieces.len();
            let a = &self.pieces[len - 2];
            let b = &self.pieces[len - 1];
            if a.len() == b.len() && a.offset % (a.len() * 2) == 0 {
                // we can merge, remove last item

                let b: merge::MergeResult<'a, T> = self.pieces.pop().unwrap();
                // let a = &mut self.pieces.last_mut().unwrap();
                // we need to temporarily remove this item to avoid lifetime and merge issues
                let mut a: merge::MergeResult<'a, T> = self.pieces.pop().unwrap();
                // that's where it needs to be inserted again
                let index = self.pieces.len();
                assert_eq!(a.offset + a.len(), b.offset);

                rayon::subgraph("merging", a.len() + b.len(), || a.merge(b, Some(self)));

                self.pieces.insert(index, a);
                // We inserted the element, we need to check with the neighbors
                if index != self.pieces.len() - 1 {
                    // that means while merging we got more pieces. We now need to merge
                    // from the inside
                    self.merge_index(index);
                }
            } else {
                break; // nothing to do
            }
        }
    }
    fn merge_index(&mut self, mut index: usize) {
        assert!(false);
        // merge neighbors of the sorted piece at index i
        let mut change = true;
        while change {
            change = false;
            let a = &self.pieces[index];
            // let b = &self.pieces[index + 1];
            if index < self.pieces.len() - 1
                && a.len() == self.pieces[index + 1].len()
                && a.offset % (a.len() * 2) == 0
            {
                // merge right neighbor
                change = true;
                let b = self.pieces.remove(index + 1);
                let a = &mut self.pieces[index];
                assert_eq!(a.offset + a.len(), b.offset);
                rayon::subgraph("merge_repair", a.len() + b.len(), || a.merge(b, None));
            } else {
                if index > 0
                    && a.len() == self.pieces[index - 1].len()
                    && a.offset % (a.len() * 2) != 0
                {
                    // merge left neighbor
                    change = true;
                    let b = self.pieces.remove(index);
                    let a = &mut self.pieces[index - 1];
                    // assert_eq!(a.offset % (a.len() * 2), 0);
                    assert_eq!(a.offset + a.len(), b.offset);
                    // assert_eq!(a.in_data, b.in_data);
                    rayon::subgraph("merge_repair", a.len() + b.len(), || a.merge(b, None));
                    index -= 1;
                }
            }
        }
    }

    fn split(self: &mut Self, _steal_counter: Option<usize>) -> Self {
        // println!("Splitting");
        // split the data in two, sort them in two tasks
        let elem_left = self.data.len();
        // we want to split off about half the slice, but also the right part needs to be a
        // power of two, so we take the slice, find the next power of two, and give half of
        // that to the other task. That means the other task will get more work.
        let split_index = elem_left.next_power_of_two() / 2;

        // split off a part for the other task
        let right_to = cut_off_right(&mut self.to, elem_left - split_index);
        let right_data = cut_off_right(&mut self.data, elem_left - split_index);
        // println!("Splitting {} in {} vs {}", total, a.len() + index, b.len());

        // Other side
        let other: Mergesort<'a, T> = Mergesort {
            pieces: Vec::new(),
            data: right_data,
            to: right_to,
            offset: self.offset + (elem_left - split_index),
        };

        return other;
    }

    fn mergesort(&mut self) {
        assert!(!self.data.is_empty());
        assert_eq!(self.data.len(), self.to.len());
        while !self.data.is_empty() {
            // let steal_counter = steal::get_my_steal_count();
            // if steal_counter > 0 && elem_left > *MIN_SPLIT_SIZE {
            //     self.split_or_run(Some(steal_counter));
            //     return;
            // }
            if self.check() {
                return;
            }
            let elem_left = self.data.len();
            // Do some work: Split off and sort piece
            // assert!(elem_left >= *MIN_BLOCK_SIZE);
            let work_size = std::cmp::min(*MIN_BLOCK_SIZE, elem_left);
            let piece = cut_off_left(&mut self.data, work_size);
            rayon::subgraph("actual sort", *MIN_BLOCK_SIZE, || piece.sort());
            let buffer = cut_off_left(&mut self.to, work_size);
            let merge = merge::MergeResult::new(piece, buffer, self.offset);
            self.offset += *MIN_BLOCK_SIZE;
            self.pieces.push(merge);
            // try merging pieces
            self.merge();
        }
        return;
    }
}
impl<'a, T> merge::Task for Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy + std::fmt::Debug,
{
    fn run(&mut self, _parent: Option<&mut dyn task::Task>) {
        self.mergesort();

    }
    fn split(&mut self) -> Self {
        Mergesort::split(self, None)
    }
    fn can_split(&self) -> bool {
        return self.data.len() > *MIN_SPLIT_SIZE;
    }
    fn fuse(&mut self, mut other: Self) {
        self.pieces.append(&mut other.pieces);
        self.merge();
    }
}
