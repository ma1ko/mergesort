#[macro_use]
extern crate lazy_static;
extern crate crossbeam_utils;
use crossbeam::CachePadded;
use crossbeam_utils as crossbeam;
use rayon_logs as rayon;
use std::option::Option;
use std::sync::atomic::{AtomicUsize, Ordering};

use itertools::Itertools;
pub const NUM_THREADS: usize = 4;
lazy_static! {
    static ref V: Vec<CachePadded<AtomicUsize>> = (0..NUM_THREADS)
        .map(|_| CachePadded::new(AtomicUsize::new(0)))
        .collect();
}
pub fn steal(backoffs: usize, victim: usize) -> Option<()> {
    let thread_index = rayon::current_thread_index().unwrap();
    let thread_index = 1 << thread_index;
    V[victim].fetch_or(thread_index, Ordering::Relaxed);
    //V[victim].fetch_add(1, Ordering::Relaxed);

    let backoff = crossbeam::Backoff::new();
    let mut c: usize;
    for _ in 0..backoffs {
        backoff.spin(); // spin or snooze()?

        // wait until the victim has taken the value, check regularly
        c = V[victim].load(Ordering::Relaxed);
        if c == 0 {
            return Some(());
        }
    }

    V[victim].fetch_and(!thread_index, Ordering::Relaxed);
    //let i = V[victim].fetch_sub(1, Ordering::Relaxed);

    //let _ = V[victim].compare_exchange_weak(c, c - 1, Ordering::Relaxed, Ordering::Relaxed);

    None
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v1: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(50000)
        .map(|x: usize| x % 1_000_000)
        .collect();

    v1.sort();

    let mut v2: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(50000)
        .map(|x: usize| x % 1_000_000)
        .collect();
    v2.sort();
    let checksum: usize = v1.iter().sum();
    let checksum: usize = checksum + v2.iter().sum::<usize>();

    let mut buffer = &mut Vec::with_capacity(v1.len() + v2.len());
    buffer.resize(buffer.capacity(), 0);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .steal_callback(|x| steal(8, x))
        .build()?;
    pool.install(|| merge(&*v1, &*v2, &mut buffer));
    /*
    println!("{:?}", v1);
    println!("{:?}", v2);
    println!("{:?}", buffer);
    */
    assert_eq!(checksum, buffer.iter().sum::<usize>(), "failed merging");
    assert!(buffer.windows(2).all(|w| w[0] <= w[1]));
    Ok(())
}

const MIN_WORK_SIZE: usize = 100;
fn merge(mut left: &[usize], mut right: &[usize], mut buffer: &mut [usize]) {
    while !left.is_empty() {
        assert_eq!(left.len() + right.len(), buffer.len(), "wrong buffers!");
        if left.len() <= MIN_WORK_SIZE {
            left.iter()
                .merge(right)
                .zip(buffer)
                .for_each(|(e, b)| *b = *e);
            return;
        }

        let thread_index = rayon::current_thread_index().unwrap();
        let steal_counter = V[thread_index].swap(0, Ordering::Relaxed);
        if steal_counter == 0 {
            // println!("{} is at {}", thread_index, left.len());
            let (leftidx, rightidx) = split_for_merge(left, right, &|a, b| a < b, MIN_WORK_SIZE);
            let (l1, l2) = left.split_at(leftidx);
            let (r1, r2) = right.split_at(rightidx);
            let (b1, b2) = buffer.split_at_mut(leftidx + rightidx);
            left = l2;
            right = r2;
            buffer = b2;
            l1.iter().merge(r1).zip(b1).for_each(|(e, b)| *b = *e);
        } else {
            let steal_counter = steal_counter.count_ones() as usize;
            let steal_counter = std::cmp::min(steal_counter, NUM_THREADS - 1);
            let chunksize = (left.len() / steal_counter + 1) + 1;

            fn spawn(chunksize: usize, left: &[usize], right: &[usize], buffer: &mut [usize]) {
                if left.len() <= MIN_WORK_SIZE || left.len() <= chunksize {
                    merge(left, right, buffer);
                    return;
                }
                let (leftidx, rightidx) = split_for_merge(left, right, &|a, b| a < b, chunksize);
                let (l1, l2) = left.split_at(leftidx);
                let (r1, r2) = right.split_at(rightidx);
                let (b1, b2) = buffer.split_at_mut(leftidx + rightidx);

                rayon::join(|| spawn(chunksize, l2, r2, b2), || merge(l1, r1, b1));
            }
            spawn(chunksize, left, right, buffer);
            return;
        }
    }
}

fn split_for_merge<T, F>(left: &[T], right: &[T], is_less: &F, index: usize) -> (usize, usize)
where
    F: Fn(&T, &T) -> bool,
{
    let left_len = left.len();
    let right_len = right.len();

    if left_len >= right_len {
        let left_mid = index; // left_len / 2;

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
        let right_mid = index; // right_len / 2;

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
