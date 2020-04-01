#[macro_use]
extern crate lazy_static;
use crossbeam_utils as crossbeam;
mod insertion_sort;
mod kmerge_impl;
pub mod merge;
pub mod rayon;
pub mod steal;

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(1_000_0)
        .map(|x: usize| x % 1_000)
        .collect();

    let checksum: usize = v.iter().sum();

    let pool = rayon::get_thread_pool();
    let (_, log) = pool.logging_install(|| mergesort(&mut v));
    assert_eq!(checksum, v.iter().sum::<usize>(), "failed merging");
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    println!("Saving svg");
    log.save_svg("test.svg").expect("failed saving svg");
    Ok(())
}
pub fn mergesort(data: &mut [usize]) {
    let mut tmp_slice1: Vec<usize> = Vec::with_capacity(data.len());
    unsafe {
        tmp_slice1.set_len(data.len());
    }
    let in_data = rayon::subgraph("Mergesort", tmp_slice1.len(), || {
        mergesort1(data, &mut tmp_slice1)
    });

    if !in_data {
        rayon::subgraph("last copy", tmp_slice1.len(), || {
            tmp_slice1.iter().zip(data).for_each(|(t, b)| *b = *t);
        });
    };
}

fn mergesort1(mut data: &mut [usize], to: &mut [usize]) -> merge::InData {
    let mut work_size = 100;
    assert_eq!(data.len(), to.len());
    let mut pieces: Vec<&mut [usize]> = Vec::new();

    while !data.is_empty() {
        let steal_counter = steal::get_my_steal_count();
        if steal_counter > 0 && data.len() > 1000 {
            // If there's more steals than threads, just create tasks for all *other* threads
            let total: usize = data.len();
            let chunks = data
                .chunks_mut(data.len() / (steal_counter + 1) + 1)
                .peekable();
            fn spawn(
                mut chunks: std::iter::Peekable<std::slice::ChunksMut<usize>>,
                to: &mut [usize],
                location: &mut Vec<merge::InData>,
            ) {
                let chunk = chunks.next().unwrap();
                match chunks.peek() {
                    None => {
                        // finished recursion, let's do our part of the data
                        let in_data =
                            rayon::subgraph("sorting", chunk.len(), || mergesort1(chunk, to));
                        location.push(in_data);
                    }
                    Some(_) => {
                        let (left_to, right_to) = to.split_at_mut(chunk.len());
                        let (_, in_data) = rayon::join(
                            || {
                                // prepare another task for the next stealer
                                spawn(chunks, right_to, location)
                            },
                            || {
                                // let the stealer process it's part
                                let in_data = rayon::subgraph("sorting", chunk.len(), || {
                                    mergesort1(chunk, left_to)
                                });
                                in_data
                            },
                        );
                        location.push(in_data);
                    }
                }
            }
            // Split my data in the part I've already sorted and the rest (which you give to other
            // threads)
            let index = pieces.iter().map(|x| x.len()).sum(); // how many items we have sorted
            let (_, rto) = to.split_at_mut(index);
            // TODO: this is inefficient vector usage
            let mut locations = Vec::new();
            spawn(chunks, rto, &mut locations);
            locations.reverse();
            pieces.iter().for_each(|_| locations.insert(0, true));
            // let rdata = data; // we don't have ldata anymore

            // we need to merge all those chunks now
            let chunks = data.chunks_mut(total / (steal_counter + 1) + 1).peekable();
            // pieces.clear();
            chunks.for_each(|x| pieces.push(x));
            let in_data = rayon::subgraph("merging pieces", to.len(), || {
                merge::two_merge(&mut pieces, to, locations)
            });
            return in_data;

            /*
            rayon::subgraph("merging together", rtemp.len(), || {
                merge::two_merge1(ltemp, rtemp, to);
            });
            */
            /*
            rayon::subgraph("merging", to.len(), || {
                merge::two_merge(&pieces, to, data);
            });
            */
        }
        // Sort a piece
        let (left, right) = data.split_at_mut(std::cmp::min(data.len(), work_size));
        work_size += 100;
        data = right;
        left.sort();
        //insertion_sort::insertion_sort(left, &|a, b| a < b);
        pieces.push(&mut *left);
    }
    let mut loc = Vec::new();
    loc.resize(pieces.len(), true);
    rayon::subgraph("merging", to.len(), || {
        merge::two_merge(&mut pieces, to, loc)
    })
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
