#[macro_use]
extern crate lazy_static;
pub mod merge;
// pub mod rayon;
mod slice_merge;
pub mod steal;
// pub mod task;
use rand::prelude::*;

use adaptive_algorithms::rayon;
use adaptive_algorithms::Task;
// lazy_static! {
//     static ref MIN_BLOCK_SIZE: usize = std::env::var("BLOCKSIZE")
//         .map(|x| x.parse::<usize>().unwrap())
//         .unwrap_or(2usize.pow(10));
//     static ref MIN_SPLIT_SIZE: usize = 32 * *MIN_BLOCK_SIZE;
// }
pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Running");
    // let _v: Vec<usize> = std::iter::repeat_with(rand::random)
    //     .take(2usize.pow(22))
    //     .take(16000000)
    //     .map(|x: usize| x % 1_000_000)
    //     .collect();
    fn random_vec(size: usize) -> Vec<u64> {
        let mut v: Vec<u64> = (0..(size as u64)).collect();
        v.shuffle(&mut thread_rng());
        v
    }
    let mut v = random_vec(100000);

    let checksum: u64 = v.iter().cloned().sum();
    println!("Finished generating");

    #[cfg(feature = "logs")]
    {
        let pool = rayon::get_thread_pool();
        let (_, log) = pool.logging_install(|| mergesort(&mut v));
        println!("Saving log");
        log.save("test").expect("failed saving log");
        // println!("Saving svg");
        // log.save_svg("test.svg").expect("failed saving svg");
    }
    #[cfg(not(feature = "logs"))]
    {
        let pool = rayon::get_thread_pool();
        let _ = pool.install(|| mergesort(&mut v));
    }
    assert_eq!(checksum, v.iter().sum::<u64>(), "failed merging");
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    println!("Success!");
    Ok(())
}

// #[test]
pub fn test() -> Result<(), Box<dyn std::error::Error>> {
    main()
}

pub fn mergesort<T>(data: &mut [T])
where
    T: Ord + Sync + Send + Copy,
{
    let mut tmp_slice: Vec<T> = Vec::with_capacity(data.len());
    unsafe { tmp_slice.set_len(data.len()) }
    let data_ptr = data.as_mut_ptr();
    let len = data.len();
    let mut mergesort = Mergesort {
        data,
        to: &mut tmp_slice,
        pieces: Vec::new(),
        blocksize: 256,
    };
    mergesort.run_();
    // There might be many ordered non-sorted blocks left. That happens when we sort an input
    // that's not a power of two elements.
    assert!(
        mergesort.pieces_len().windows(2).all(|w| w[0] > w[1]),
        format!("{:?}", mergesort.pieces_len())
    );
    // let's merge all the pieces from the back
    while mergesort.pieces.len() >= 2 {
        let mut other = mergesort.pieces.pop().unwrap();
        let me = mergesort.pieces.last_mut().unwrap();
        unsafe {
            if me.data.as_ptr().add(me.data.len()) != other.data.as_ptr() {
                // one piece has it's result in the data and the other in the memory. We need to
                // copy one over (it's better to choose the smaller piece
                std::ptr::copy_nonoverlapping(
                    other.data.as_ptr(),
                    other.buffer.as_mut_ptr(),
                    other.data.len(),
                );
                std::mem::swap(&mut other.data, &mut other.buffer);
            }
        }
        mergesort
            .pieces
            .last_mut()
            .unwrap()
            .merge(other, adaptive_algorithms::task::NOTHING);
    }
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
pub fn cut_off_left<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = right;
    left
}
pub fn cut_off_right<'a, T>(s: &mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let mid = if mid <= s.len() {
        mid
    } else {
        println!("FAIL mid: {}, len: {}", mid, s.len());
        assert!(false);
        s.len() - 1
    };
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = left;
    right
}

struct Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    data: &'a mut [T],
    to: &'a mut [T],
    pieces: Vec<merge::MergeResult<'a, T>>,
    blocksize: usize,
}
impl<'a, T> Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
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
            if a.len() == b.len() {
                // we can merge, remove last item

                let b: merge::MergeResult<'a, T> = self.pieces.pop().unwrap();
                let a: &mut merge::MergeResult<'a, T> = &mut self.pieces.last_mut().unwrap();
                // we want to be able to work on this element while also working on the merge at
                // the same time. There should be a better that disabling the borrow checker here,
                // but it works for now
                let a: &mut merge::MergeResult<'a, T> = unsafe { std::mem::transmute(a) };

                rayon::subgraph("merging", a.len() + b.len(), || a.merge(b, Some(self)));
            // rayon::subgraph("merging", a.len() + b.len(), || a.merge(b, task::NOTHING));
            } else {
                break; // nothing to do
            }
        }
    }

    fn next(&self, i: usize) -> usize {
        // find the next number that has only leading zeros in binary
        // eg next(5) = next(0b101) = 0b110
        // next(9) = next(0x1001) = 0x1100 = 12
        // we need to have an amount of elements like this on the left to ensure that we can merge
        // the result in the end
        let mut highest = i.next_power_of_two() / 2;
        // find first zero
        while highest != 0 && i | highest == i {
            highest /= 2;
        }
        let result = i & !(highest - 1);
        if i != result {
            result | highest
        } else {
            result
        }
    }
}
use std::vec::Vec;
impl<'a, T> Task for Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    fn step(&mut self) {
        // this seems to be required after a split sometimes
        self.merge();

        let elem_left = self.data.len();
        if elem_left == 0 {
            return;
        };
        // Do some work: Split off and sort piece
        let work_size = std::cmp::min(self.blocksize, elem_left);
        let piece = cut_off_left(&mut self.data, work_size);
        rayon::subgraph("actual sort", self.blocksize, || piece.sort());
        let buffer = cut_off_left(&mut self.to, work_size);
        let merge = merge::MergeResult::new(piece, buffer);
        self.pieces.push(merge);
        // try merging pieces
        self.merge();

        return;
    }
    fn is_finished(&self) -> bool {
        self.data.is_empty()
    }
    fn split(&mut self, mut runner: impl FnMut(&mut Vec<&mut Self>), steal_counter: usize) {
        // split the data in two, sort them in two tasks
        let elem_left = self.data.len();
        // we want to split off about half the slice, but also the right part needs to be a
        // power of two, so we take the slice, find the next power of two, and give half of
        // that to the other task. That means the other task will get more work.
        let already_done = self.pieces_len().iter().sum::<usize>();
        let total = already_done + elem_left;
        let split_index = self.next(already_done.max(total.next_power_of_two() / 2));

        // split off a part for the other task
        let right_to = cut_off_right(&mut self.to, split_index - already_done);
        let right_data = cut_off_right(&mut self.data, split_index - already_done);

        // Other side
        let mut other: Mergesort<'a, T> = Mergesort {
            pieces: Vec::new(),
            data: right_data,
            to: right_to,
            blocksize: self.blocksize,
        };
        // println!("Split {} to {}", self.data.len(), other.data.len());
        runner(&mut vec![self, &mut other]);
    }
    fn can_split(&self) -> bool {
        return self.data.len() > self.blocksize * 32;
    }
    fn fuse(&mut self, other: &mut Self) {
        self.merge();
        self.pieces.append(&mut other.pieces);
        self.merge();
    }
}
