#[macro_use]
extern crate lazy_static;
use crossbeam_utils as crossbeam;
mod insertion_sort;
mod kmerge_impl;
mod merge;
mod rayon;
pub mod steal;

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(10_000_000)
        .map(|x: usize| x % 1_000_000)
        .collect();

    let checksum: usize = v.iter().sum();

    let pool = rayon::get_thread_pool();
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

    rayon::subgraph("last copy", tmp_slice1.len(), || {
        tmp_slice1.iter().zip(data).for_each(|(t, b)| *b = *t);
    });
}

fn mergesort1(mut data: &mut [usize], to: &mut [usize], temp: &mut [usize]) {
    let mut work_size = 20;
    // assert_eq!(data.len(), to.len());
    // assert_eq!(data.len(), temp.len());
    let mut pieces: Vec<&[usize]> = Vec::new();

    while !data.is_empty() {
        let steal_counter = steal::get_my_steal_count();
        if steal_counter > 0 && data.len() > 1000 {
            // If there's more steals than threads, just create tasks for all *other* threads
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
                        rayon::subgraph("sorting", chunk.len(), || {
                            mergesort1(chunk, to, temp);
                        });
                    }
                    Some(_) => {
                        let (left_to, right_to) = to.split_at_mut(chunk.len());
                        let (left_temp, right_temp) = temp.split_at_mut(chunk.len());
                        rayon::join(
                            || {
                                // prepare another task for the next stealer
                                spawn(chunks, right_to, right_temp);
                            },
                            || {
                                // let the stealer process it's part
                                rayon::subgraph("sorting", chunk.len(), || {
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
            rayon::subgraph("merging", to.len(), || {
                merge::merge(pieces, to);
            });
            return;
        }
        // Sort a piece
        let (left, right) = data.split_at_mut(std::cmp::min(data.len(), work_size));
        work_size += 100;
        data = right;
        left.sort();
        //insertion_sort::insertion_sort(left, &|a, b| a < b);
        pieces.push(&*left);
    }
    rayon::subgraph("merging", to.len(), || {
        merge::merge(pieces, to);
    });
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
