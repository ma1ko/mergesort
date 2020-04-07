#[macro_use]
extern crate lazy_static;
use crossbeam_utils as crossbeam;
mod kmerge_impl;
pub mod merge;
pub mod rayon;
pub mod steal;

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        .map(|x: usize| x % 1_000_000)
        .collect();
    // let mut v: Vec<usize> = (0..2usize.pow(20)).into_iter().collect();
    // v.reverse();

    let checksum: usize = v.iter().sum();

    let pool = rayon::get_thread_pool();
    let (_, log) = pool.logging_install(|| mergesort(&mut v));
    assert_eq!(checksum, v.iter().sum::<usize>(), "failed merging");
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    println!("Saving log");
    // log.save("test").expect("failed saving log");
    println!("Saving svg");
    // log.save_svg("test.svg").expect("failed saving svg");
    Ok(())
}
pub fn mergesort<T>(data: &mut [T])
where
    T: Ord + Sync + Send + Copy + Default,
{
    let mut tmp_slice: Vec<T> = Vec::with_capacity(data.len());
    unsafe { tmp_slice.set_len(data.len()) }
    let in_data = rayon::subgraph("sorting", tmp_slice.len(), || {
        let mut pieces = Vec::new();
        mergesort1(data, &mut tmp_slice, &mut pieces);
        pieces[0].in_data
    });

    if !in_data {
        rayon::subgraph("last copy", tmp_slice.len(), || {
            tmp_slice.iter().zip(data).for_each(|(t, b)| *b = *t);
        });
    };
}
fn merge_neighbors<T>(pieces: &mut Vec<merge::MergeResult<T>>)
where
    T: Ord + Sync + Send + Copy,
{
    while pieces.len() >= 2 {
        // to merge we need at least two parts
        let len = pieces.len();
        let a = &pieces[len - 2];
        let b = &pieces[len - 1];
        if a.len() == b.len() {
            // we can merge
            let b = pieces.pop().unwrap(); //remove the last
            let a = &mut pieces.last_mut().unwrap();
            assert_eq!(a.in_data, b.in_data);
            assert_eq!(a.len(), b.len());

            // rayon::subgraph("merging", a.len() + b.len(), || a.merge(b));
            a.merge(b);
        } else {
            // println!("Couldn't merge {} and {}", a.len(), b.len());
            break; // nothing to do
        }
    }
}
fn mergesort1<'a, T>(
    mut data: &'a mut [T],
    mut to: &'a mut [T],
    mut pieces: &mut Vec<merge::MergeResult<'a, T>>,
) where
    T: Ord + Sync + Send + Copy,
{
    assert!(!data.is_empty());
    assert_eq!(data.len(), to.len());
    // How much is currently sorted
    let mut index: usize = pieces.iter().map(|x| x.len()).sum::<usize>();
    // Total amount of elements in the slice
    let total = data.len() + index;
    // println!("I have {} elements, plus {}", total, index);

    while total - index > 0 {
        let elem_left = data.len();
        let steal_counter = steal::get_my_steal_count();
        if steal_counter > 0 && elem_left > 4096 {
            // TODO: There's probably a smarter way to do this...
            let split_index = if index < total / 2 {
                total / 2
            } else {
                if index < total / 4 {
                    total / 4
                } else {
                    if index < total / 8 {
                        total / 8
                    } else {
                        // that's more complex here...
                        // data.len() / 4
                        // println!("Unable to steal {} from {}", index, total);
                        continue; // just ignore that steal
                    }
                }
            };
            let (left_to, right_to) = to.split_at_mut(split_index - index);
            let (a, b) = data.split_at_mut(split_index - index);
            // println!("Splitting in {} vs {}", a.len(), b.len());
            let mut other_pieces = Vec::new();
            // TODO: understand the lifetimes issues here
            let (mut pieces, mut other_pieces) = rayon::join(
                move || {
                    rayon::subgraph("sorting", split_index, move || {
                        mergesort1(a, left_to, pieces);
                        return pieces;
                    })
                },
                move || {
                    rayon::subgraph("sorting", split_index, move || {
                        mergesort1(b, right_to, &mut other_pieces);
                        return other_pieces;
                    })
                },
            );
            // we need to merge all those chunks now
            pieces.append(&mut other_pieces);
            merge_neighbors(&mut pieces);
            assert_eq!(pieces.len(), 1);
            return;
        }
        // Do some work: Split off and sort piece
        let work_size = std::cmp::min(256, elem_left);
        let (piece, rest) = data.split_at_mut(work_size);
        data = rest;
        piece.sort();
        let (buffer, rest) = to.split_at_mut(work_size);
        to = rest;
        let merge = merge::MergeResult::new(piece, buffer, true);
        index += work_size;
        pieces.push(merge);
        // try merging pieces
        merge_neighbors(&mut pieces);
    }
    assert_eq!(pieces.len(), 1);
    return;
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
