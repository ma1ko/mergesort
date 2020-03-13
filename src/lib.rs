#[macro_use]
extern crate lazy_static;
extern crate crossbeam_utils;
use crossbeam::CachePadded;
use crossbeam_utils as crossbeam;
use itertools::kmerge;
use rayon_logs as rayon;
use std::collections::BinaryHeap;
use std::option::Option;
use std::sync::atomic::{AtomicUsize, Ordering};

use itertools::Itertools;
pub const NUM_THREADS: usize = 2;
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

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
struct Index<'a> {
    index: usize,
    slice: &'a [usize],
}
impl Index<'_> {
    fn get(self: &Self) -> Option<&usize> {
        self.slice.get(self.index)
    }
}

impl PartialOrd for Index<'_> {
    fn partial_cmp(self: &Self, other: &Index) -> Option<std::cmp::Ordering> {
        // We turn aronud the binary heap to get minimum comparisions
        //other.get().partial_cmp(&self.get())
        self.get().partial_cmp(&other.get())
    }
}
impl Ord for Index<'_> {
    fn cmp(self: &Self, other: &Index) -> std::cmp::Ordering {
        //self.partial_cmp(other).unwrap()
        other.partial_cmp(self).unwrap()
    }
}

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v1: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(10000)
        .map(|x: usize| x % 1_000)
        .collect();

    // v1.sort();
    // v1.reverse();

    // let mut v2: Vec<usize> = std::iter::repeat_with(rand::random)
    //     .take(200000)
    //     .map(|x: usize| x % 1_000_000)
    //     .collect();
    // v2.sort();
    // v2.reverse();
    let checksum: usize = v1.iter().sum();
    // let checksum: usize = checksum + v2.iter().sum::<usize>();

    let mut buffer = &mut Vec::with_capacity(v1.len());
    buffer.resize(buffer.capacity(), 0);

    let pool = rayon_logs::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .steal_callback(|x| steal(8, x))
        .build()?;
    /*
    let mut v: Vec<&[usize]> = Vec::new();
    v.push(&v1);
    v.push(&v2);
    */
    let (_, log) = pool.logging_install(|| mergesort(&mut v1, &mut buffer));
    log.save_svg("test.svg").expect("failed saving svg");

    assert_eq!(checksum, buffer.iter().sum::<usize>(), "failed merging");
    println!("{:?}", v1);
    assert!(buffer.windows(2).all(|w| w[0] >= w[1]));

    Ok(())
}

fn mergesort(mut data: &mut [usize], mut buffer: &mut [usize]) {
    let orig = data.as_mut_ptr();
    let orig_size = data.len();
    assert_eq!(data.len(), buffer.len());
    let thread_index = rayon::current_thread_index().unwrap();
    let mut pieces: Vec<&[usize]> = Vec::new();

    while !data.is_empty() {
        let steal_counter = V[thread_index].swap(0, Ordering::Relaxed);
        let steal_counter = steal_counter.count_ones() as usize;
        if steal_counter > 0 && data.len() > 1000 {
            // If there's more steals than threads, just create tasks for all *other* threads
            let steal_counter = std::cmp::min(steal_counter, NUM_THREADS - 1);
            let chunks_ptr = data.as_mut_ptr();
            let chunks_len = data.len();
            let chunks = data
                .chunks_mut(data.len() / (steal_counter + 1) + 1)
                .peekable();
            println!("{} is Splitting in {} chunks", thread_index, chunks.len());
            fn spawn(
                mut chunks: std::iter::Peekable<std::slice::ChunksMut<usize>>,
                buffer: &mut [usize],
            ) {
                let chunk = chunks.next().unwrap();
                match chunks.peek() {
                    None => {
                        // finished recursion, let's do our part of the data
                        let thread_index = rayon::current_thread_index().unwrap();
                        println!("{}: mergesort({})", thread_index, chunk.len());
                        assert_eq!(chunk.len(), buffer.len());
                        mergesort(chunk, buffer);
                    }
                    Some(_) => {
                        let (lb, rb) = buffer.split_at_mut(chunk.len());
                        rayon_logs::join(
                            || {
                                // prepare another task for the next stealer
                                spawn(chunks, rb);
                            },
                            || {
                                // let the stealer process it's part
                                assert_eq!(chunk.len(), lb.len());
                                mergesort(chunk, lb);
                            },
                        );
                    }
                };
            }
            let index = pieces.iter().map(|x| x.len()).sum();
            let (lb, rb) = buffer.split_at_mut(index);
            spawn(chunks, rb);
            let chunks_data = unsafe { std::slice::from_raw_parts_mut(chunks_ptr, chunks_len) };
            assert!(chunks_data.windows(2).all(|w| w[0] >= w[1]));
            merge(pieces, lb);
            assert!(lb.windows(2).all(|w| w[0] >= w[1]));
            let x = unsafe { std::slice::from_raw_parts_mut(orig, orig_size) };
            merge(vec![chunks_data, lb], x);
            return;
        }

        let (left, right) = data.split_at_mut(std::cmp::min(data.len(), MIN_WORK_SIZE));
        data = right;
        left.sort();
        left.reverse();
        pieces.push(&*left);
    }
    println!("{} is merging", thread_index);
    merge(pieces, buffer);
    println!("{} is finished merging", thread_index);
    //data.iter_mut().zip(buffer).for_each(|(d, b)| *d = *b);
}

const MIN_WORK_SIZE: usize = 100;
fn merge(slices: Vec<&[usize]>, buffer: &mut [usize]) {
    assert_eq!(slices.iter().map(|x| x.len()).sum::<usize>(), buffer.len());
    let mut heap: BinaryHeap<Index> = BinaryHeap::new();
    slices.iter().for_each(|slice| {
        heap.push(Index {
            index: 0,
            slice: slice,
        })
    });

    let mut buffer = buffer.iter_mut();
    while !heap.is_empty() {
        // assert_eq!(left.len() + right.len(), buffer.len(), "wrong buffers!");
        /*
        if num_elements <= MIN_WORK_SIZE {
            kmerge(slices.into_iter())
                .zip(buffer)
                .for_each(|(i, b)| *b = *i);
            return;
        }
        */

        let thread_index = rayon::current_thread_index().unwrap();
        let steal_counter = V[thread_index].swap(0, Ordering::Relaxed);
        if steal_counter == 0 || buffer.len() < MIN_WORK_SIZE {
            // Do a part of the work
            let mut work = 0;
            while let Some(mut val) = heap.peek_mut() {
                work += 1;
                if work == MIN_WORK_SIZE {
                    break;
                }
                if val.get().is_none() {
                    return;
                };

                let pos = buffer.next().unwrap();
                *pos = *val.get().unwrap();

                *val = Index {
                    index: val.index + 1,
                    slice: val.slice,
                };
            }
        } else {
            let steal_counter = steal_counter.count_ones() as usize;
            let steal_counter = std::cmp::min(steal_counter, NUM_THREADS - 1);
            /*
            let max_index = heap.iter().max_by_key(|index| index.index).unwrap().index;
            let shortest = slices.iter().min_by_key(|slice| slice.len()).unwrap().len();
            if max_index >= shortest {
                continue; // we are mostly finished, nobody get's anything
            }
            */
            let slices: Vec<&[usize]> = heap
                .drain()
                .map(|index| {
                    let (_, right) = index.slice.split_at(index.index);
                    right
                })
                .collect();
            let max_slice = slices.iter().max_by_key(|&index| index.len()).unwrap();
            let buffer = buffer.into_slice();
            // let chunksize = max_slice.len() / (steal_counter + 1) + 1;

            fn spawn(steal_counter: usize, slices: Vec<&[usize]>, buffer: &mut [usize]) {
                assert_eq!(slices.iter().map(|x| x.len()).sum::<usize>(), buffer.len());
                let max_slice = slices.iter().max_by_key(|&index| index.len()).unwrap();
                if steal_counter == 1 {
                    // Just me left
                    let thread_index = rayon::current_thread_index().unwrap();
                    println!(
                        "{}: merge({}), buffer: {}",
                        thread_index,
                        max_slice.len(),
                        buffer.len()
                    );
                    merge(slices, buffer);
                    return;
                }
                let split = max_slice.len() / steal_counter;
                let split_elem = max_slice[split];
                let splits: Vec<(&[usize], &[usize])> = slices
                    .iter()
                    .map(|slice| {
                        let index = split_for_merge(slice, &|a, b| a > b, &split_elem);
                        slice.split_at(index)
                    })
                    .collect();

                let (left, right): (Vec<_>, Vec<_>) = splits.iter().cloned().unzip();

                let (b1, b2) = buffer.split_at_mut(left.iter().map(|vec| vec.len()).sum());

                rayon_logs::join(|| spawn(steal_counter - 1, right, b2), || merge(left, b1));
            }
            spawn(steal_counter + 1 /* me */, slices, buffer);
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
