#[macro_use]
extern crate lazy_static;
use crossbeam_utils as crossbeam;
pub mod merge;
pub mod rayon;
pub mod steal;

pub fn main_tuple() -> Result<(), Box<dyn std::error::Error>> {
    let mut v: Vec<Tuple> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        .enumerate()
        .map(|(x, y): (usize, usize)| Tuple {
            left: y % 10,
            right: x,
        })
        .collect();
    let pool = rayon::get_thread_pool();
    let (_, log) = pool.logging_install(|| mergesort(&mut v));
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    assert!(v
        .windows(2)
        .all(|w| w[0] != w[1] || w[0].right <= w[1].right));
    println!("Saving log");
    log.save("test").expect("failed saving log");
    println!("Saving svg");
    // log.save_svg("test.svg").expect("failed saving svg");
    Ok(())
}

#[derive(Default, Copy, Clone, Debug)]
struct Tuple {
    left: usize,
    right: usize,
}
impl PartialEq for Tuple {
    fn eq(&self, other: &Tuple) -> bool {
        return self.left == other.left;
    }
}
impl Eq for Tuple {}

use std::cmp::Ordering;
impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Tuple) -> Option<Ordering> {
        self.left.partial_cmp(&other.left)
    }
}
impl Ord for Tuple {
    fn cmp(&self, other: &Tuple) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut v: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        .map(|x: usize| x % 2)
        .collect();
    // let mut v: Vec<usize> = (0..2usize.pow(20)).into_iter().collect();
    // v.reverse();

    let checksum: usize = v.iter().sum();

    let pool = rayon::get_thread_pool();
    let (_, log) = pool.logging_install(|| mergesort(&mut v));
    assert_eq!(checksum, v.iter().sum::<usize>(), "failed merging");
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    // println!("Saving log");
    log.save("test").expect("failed saving log");
    // println!("Saving svg");
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
        let mut mergesort = Mergesort {
            data: data,
            to: &mut tmp_slice,
            pieces: Vec::new(),
        };
        mergesort.mergesort();
        assert!(
            mergesort.pieces.len() == 1,
            format!(
                "{:?}",
                mergesort
                    .pieces
                    .iter()
                    .map(|x| x.len())
                    .collect::<Vec<usize>>()
            )
        );
        mergesort.pieces[0].in_data
    });

    if !in_data {
        rayon::subgraph("last copy", tmp_slice.len(), || {
            tmp_slice.iter().zip(data).for_each(|(t, b)| *b = *t);
        });
    };
}
// from https://stackoverflow.com/questions/42162151/rust-error-e0495-using-split-at-mut-in-a-closure
fn cut_off_piece<'a, T>(s: &'a mut &'a mut [T], mid: usize) -> &'a mut [T] {
    let tmp: &'a mut [T] = ::std::mem::replace(&mut *s, &mut []);
    let (left, right) = tmp.split_at_mut(mid);
    *s = right;
    left
}

struct Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    data: &'a mut [T],
    to: &'a mut [T],
    pieces: Vec<merge::MergeResult<'a, T>>,
}
// fn mergesort_split<T>(data: &mut [T], to: &mut [T]) -> bool
// where
//     T: Ord + Sync + Send + Copy,
// {
//     true
// }
impl<'a, T> Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    // fn merge(&mut self) {
    //     merge_neighbors(&mut self.pieces);
    // }
    fn merge(&mut self)
    where
        T: Ord + Sync + Send + Copy,
    {
        while self.pieces.len() >= 2 {
            // to merge we need at least two parts
            let len = self.pieces.len();
            let a = &self.pieces[len - 2];
            let b = &self.pieces[len - 1];
            if a.len() == b.len() {
                // we can merge
                let b = self.pieces.pop().unwrap(); //remove the last
                let a = &mut self.pieces.last_mut().unwrap();
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

    fn mergesort(self: &mut Self) {
        assert!(!self.data.is_empty());
        assert_eq!(self.data.len(), self.to.len());
        while !self.data.is_empty() {
            let elem_left = self.data.len();
            let steal_counter = steal::get_my_steal_count();
            if steal_counter > 0 && elem_left > 4096 {
                // let prev_split_index = total / elem_left.next_power_of_two();
                let split_index = elem_left.next_power_of_two() / 2;

                // https://stackoverflow.com/questions/42162151/rust-error-e0495-using-split-at-mut-in-a-closure
                // always split from the back
                let to: &'a mut [T] = std::mem::replace(&mut self.to, &mut []);
                let (left_to, right_to) = to.split_at_mut(elem_left - split_index);
                self.to = left_to;
                // let right_to = cut_off_piece(&mut self.to, elem_left - split_index);
                let data: &'a mut [T] = std::mem::replace(&mut self.data, &mut []);
                let (left_data, right_data) = data.split_at_mut(elem_left - split_index);
                self.data = left_data;
                // println!("Splitting {} in {} vs {}", total, a.len() + index, b.len());

                // Other side
                let mut other: Mergesort<'a, T> = Mergesort {
                    pieces: Vec::new(),
                    data: right_data,
                    to: right_to,
                };
                // TODO: understand the lifetimes issues here
                let (_, _) = rayon::join(
                    || {
                        // rayon::subgraph("sorting", split_index, move || {
                        self.mergesort();
                        // })
                    },
                    || {
                        // rayon::subgraph("sorting", split_index, move || {
                        other.mergesort();
                        // })
                    },
                );
                // we need to merge all those chunks now
                // if !self.pieces.is_empty() && !other.pieces.is_empty() {
                //     assert!(
                //         self.pieces.last().unwrap().len() >= other.pieces.first().unwrap().len(),
                //         format!(
                //             "{:?} vs {:?}",
                //             self.pieces.iter().map(|x| x.len()).collect::<Vec<usize>>(),
                //             other.pieces.iter().map(|x| x.len()).collect::<Vec<usize>>()
                //         )
                //     );
                // }
                self.pieces.append(&mut other.pieces);
                self.merge();
                return;
            }
            // Do some work: Split off and sort piece
            let work_size = std::cmp::min(256, elem_left);
            let tmp: &'a mut [T] = std::mem::replace(&mut self.data, &mut []);
            let (piece, rest) = tmp.split_at_mut(work_size);
            self.data = rest;
            piece.sort();
            let tmp: &'a mut [T] = std::mem::replace(&mut self.to, &mut []);
            let (buffer, rest) = tmp.split_at_mut(work_size);
            self.to = rest;
            let merge = merge::MergeResult::new(piece, buffer, true);
            self.pieces.push(merge);
            // try merging pieces
            self.merge();
        }
        return;
    }
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
