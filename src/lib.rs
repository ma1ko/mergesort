#[macro_use]
extern crate lazy_static;
extern crate crossbeam_utils;
use crossbeam::CachePadded;
use crossbeam_utils as crossbeam;
use rayon_logs as rayon;
use std::option::Option;
use std::sync::atomic::{AtomicUsize, Ordering};
pub const NUM_THREADS: usize = 4;
mod kmerge_impl;
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
    let mut v: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(1000000)
        .map(|x: usize| x % 1_000_000)
        .collect();

    let checksum: usize = v.iter().sum();

    let pool = rayon_logs::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .steal_callback(|x| steal(8, x))
        .build()?;

    let (_, log) = pool.logging_install(|| mergesort(&mut v));
    log.save_svg("test.svg").expect("failed saving svg");

    assert_eq!(checksum, v.iter().sum::<usize>(), "failed merging");

    // println!("{:?}", v);
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    Ok(())
}
pub fn mergesort(data: &mut [usize]) {
    let mut tmp_slice1: Vec<usize> = Vec::with_capacity(data.len());
    let mut tmp_slice2: Vec<usize> = Vec::with_capacity(data.len());
    unsafe {
        tmp_slice1.set_len(data.len());
        tmp_slice2.set_len(data.len());
    }
    mergesort1(data, &mut tmp_slice1, &mut tmp_slice2);

    rayon_logs::subgraph("last copy", tmp_slice1.len(), || {
        tmp_slice1.iter().zip(data).for_each(|(t, b)| *b = *t);
    });
}

fn mergesort1(mut data: &mut [usize], to: &mut [usize], temp: &mut [usize]) {
    // println!("Sorting");
    // assert_eq!(data.len(), to.len());
    // assert_eq!(data.len(), temp.len());
    let thread_index = rayon::current_thread_index().unwrap();
    let mut pieces: Vec<&[usize]> = Vec::new();

    while !data.is_empty() {
        let steal_counter = V[thread_index].swap(0, Ordering::Relaxed);
        let steal_counter = steal_counter.count_ones() as usize;
        if steal_counter > 0 && data.len() > 1000 {
            // If there's more steals than threads, just create tasks for all *other* threads
            let steal_counter = std::cmp::min(steal_counter, NUM_THREADS - 1);
            let chunks = data
                .chunks_mut(data.len() / (steal_counter + 1) + 1)
                .peekable();
            fn spawn(
                mut chunks: std::iter::Peekable<std::slice::ChunksMut<usize>>,
                to: &mut [usize],
                temp: &mut [usize],
            ) {
                let chunk = chunks.next().unwrap();
                match chunks.peek() {
                    None => {
                        // finished recursion, let's do our part of the data
                        // assert_eq!(chunk.len(), to.len());
                        rayon_logs::subgraph("sorting", chunk.len(), || {
                            mergesort1(chunk, to, temp);
                        });
                    }
                    Some(_) => {
                        let (left_to, right_to) = to.split_at_mut(chunk.len());
                        let (left_temp, right_temp) = temp.split_at_mut(chunk.len());
                        rayon_logs::join(
                            || {
                                // prepare another task for the next stealer
                                spawn(chunks, right_to, right_temp);
                            },
                            || {
                                // let the stealer process it's part
                                // assert_eq!(chunk.len(), left_to.len());
                                // assert_eq!(chunk.len(), left_temp.len());
                                rayon_logs::subgraph("sorting", chunk.len(), || {
                                    mergesort1(chunk, left_to, left_temp);
                                });
                            },
                        );
                    }
                };
            }
            // Split my data in the part I've already sorted and the rest (which you give to other
            // threads)
            let index = pieces.iter().map(|x| x.len()).sum(); // how many items we have sorted
            let (_, rto) = to.split_at_mut(index);
            let (_, rtemp) = temp.split_at_mut(index);

            spawn(chunks, rtemp, rto);
            // we need to merge all those chunks now
            let chunks = rtemp
                .chunks_mut(data.len() / (steal_counter + 1) + 1)
                .peekable();
            chunks.for_each(|x| pieces.push(x));
            // assert!(pieces.iter().all(|x| x.windows(2).all(|w| w[0] <= w[1])));
            rayon_logs::subgraph("merging", to.len(), || {
                merge(pieces, to);
            });
            // assert!(to.windows(2).all(|w| w[0] <= w[1]));
            return;
        }
        // Sort a piece
        let (left, right) = data.split_at_mut(std::cmp::min(data.len(), MIN_WORK_SIZE));
        data = right;
        left.sort();
        pieces.push(&*left);
    }
    rayon_logs::subgraph("merging", to.len(), || {
        merge(pieces, to);
    });
    // assert!(to.windows(2).all(|w| w[0] <= w[1]));
}
// Mabye we can rewrite it a bit more like that
// pub fn recursive_join<I, T, F>(it: I, f: F)
// where
//     T: Send,
//     I: Iterator<Item = T> + Send,
//     F: Fn(T) + Send + Sync,
// {
//     let it = it.into_iter();
//     fn spawn<I, T, F>(mut it: I, f: F)
//     where
//         T: Send,
//         I: Iterator<Item = T> + Send,
//         F: Fn(T) + Send + Sync,
//     {
//         match it.next() {
//             None => {}
//             Some(t) => {
//                 rayon_logs::join(
//                     || {
//                         spawn(it, &f);
//                     },
//                     || {
//                         f(t);
//                     },
//                 );
//             }
//         };
//     }
//     spawn(it, f);
// }

const MIN_WORK_SIZE: usize = 5000;
pub fn merge(slices: Vec<&[usize]>, buffer: &mut [usize]) {
    let slice_iters = slices.iter().map(|x| x.iter());
    let mut iter = kmerge_impl::kmerge(slice_iters);

    let mut buffer = buffer.iter_mut();
    //while !buffer.peekable().peek().is_some() {
    loop {
        let thread_index = rayon_logs::current_thread_index().unwrap();
        let steal_counter = V[thread_index].swap(0, Ordering::Relaxed);
        if steal_counter == 0 || buffer.len() < MIN_WORK_SIZE {
            // Do a part of the work
            for _ in 0..MIN_WORK_SIZE {
                match buffer.next() {
                    Some(buf) => *buf = *iter.next().unwrap(),
                    None => return,
                }
            }
        } else {
            let steal_counter = steal_counter.count_ones() as usize;
            let steal_counter = std::cmp::min(steal_counter, NUM_THREADS - 1);
            // Someone is trying to steal. We need to recover the slices from the merging.
            let slices = iter
                .heap
                .iter_mut()
                .map(|headtail| {
                    // kmerge has a structing with one head element and tail iterator
                    // that's the tail
                    let slice = headtail.tail.as_slice();
                    unsafe {
                        // we now get the head by constructing a slice that's one element larger at
                        // the front
                        let start = slice.get_unchecked(0) as *const usize;
                        let start = start.offset(-1);
                        let len = slice.len() + 1;
                        std::slice::from_raw_parts(start, len)
                    }
                })
                .collect();

            // The rest of the buffer
            let buffer = buffer.into_slice();

            fn spawn(steal_counter: usize, slices: Vec<&[usize]>, buffer: &mut [usize]) {
                // assert_eq!(slices.iter().map(|x| x.len()).sum::<usize>(), buffer.len());
                let max_slice = slices.iter().max_by_key(|&index| index.len()).unwrap();
                if steal_counter == 1 || max_slice.len() < MIN_WORK_SIZE / slices.len() {
                    // Just me left
                    // let thread_index = rayon::current_thread_index().unwrap();
                    // println!(
                    //     "{}: merge({}), buffer: {}",
                    //     thread_index,
                    //     max_slice.len(),
                    //     buffer.len()
                    // );
                    merge(slices, buffer);
                    // assert!(buffer.windows(2).all(|w| w[0] <= w[1]));
                    return;
                }
                let split = max_slice.len() / steal_counter;
                if split == 0 {
                    println!("max: {}, {}", max_slice.len(), buffer.len());
                }
                // the element to split
                let split_elem = max_slice[split];

                // find the splitting points in all splices
                let splits: Vec<(&[usize], &[usize])> = slices
                    .iter()
                    .map(|slice| {
                        let index = split_for_merge(slice, &|a, b| a < b, &split_elem);
                        slice.split_at(index)
                    })
                    .collect();

                let (left, right): (Vec<_>, Vec<_>) = splits.iter().cloned().unzip();
                // split the buffer at the sum of all left splits length (so they are the same size
                let (b1, b2) = buffer.split_at_mut(left.iter().map(|vec| vec.len()).sum());
                rayon::join(|| spawn(steal_counter - 1, right, b2), || merge(left, b1));
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
