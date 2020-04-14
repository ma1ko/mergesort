#[macro_use]
extern crate lazy_static;
use crossbeam_utils as crossbeam;
pub mod merge;
pub mod rayon;
pub mod steal;

/*
pub fn main_tuple() -> Result<(), Box<dyn std::error::Error>> {
    // test for stability with a Tuple where we only sort by the first element, then test if the
    // second elements stayed in the same order
    let mut v: Vec<Tuple> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(24))
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
    log.save_svg("test.svg").expect("failed saving svg");
    Ok(())
}
*/

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
        .take(2usize.pow(24))
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
    log.save("test").expect("failed saving log");
    println!("Saving svg");
    log.save_svg("test.svg").expect("failed saving svg");
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
            format!("{:?}", mergesort.pieces_len())
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
    T: Ord + Sync + Send + Copy,
{
    data: &'a mut [T],
    to: &'a mut [T],
    pieces: Vec<merge::MergeResult<'a, T>>,
}
impl<'a, T> Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    fn pieces_len(&self) -> Vec<usize> {
        self.pieces.iter().map(|x| x.len()).collect()
    }
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
                // we can merge, remove last item
                let b = self.pieces.pop().unwrap();
                // let a = &mut self.pieces.last_mut().unwrap();
                // we need to temporarily remevove this item to ovoid merge issues
                let mut a = self.pieces.pop().unwrap();
                assert_eq!(a.in_data, b.in_data);
                assert_eq!(a.len(), b.len());

                a.merge(b, Some(self));
                self.pieces.push(a);
            } else {
                break; // nothing to do
            }
        }
    }
    fn split(self: &mut Self) -> bool {
        let elem_left = self.data.len();
        if elem_left < 4096 {
            return false;
        }
        // we want to split off about half the slice, but also the right part needs to be a
        // power of two, so we take the slice, find the next power of two, and give half of
        // that to the other task. That means the other task will get more work.
        let split_index = elem_left.next_power_of_two() / 2;

        // split off a part for the other guy
        let right_to = cut_off_right(&mut self.to, elem_left - split_index);
        let right_data = cut_off_right(&mut self.data, elem_left - split_index);
        // println!("Splitting {} in {} vs {}", total, a.len() + index, b.len());

        // Other side
        let mut other: Mergesort<'a, T> = Mergesort {
            pieces: Vec::new(),
            data: right_data,
            to: right_to,
        };
        rayon::join(
            || {
                rayon::subgraph("sorting", split_index, || {
                    self.mergesort();
                })
            },
            || {
                rayon::subgraph("sorting", split_index, || {
                    other.mergesort();
                })
            },
        );
        self.pieces.append(&mut other.pieces);
        self.merge();
        return true;
    }

    fn mergesort(self: &mut Self) {
        assert!(!self.data.is_empty());
        assert_eq!(self.data.len(), self.to.len());
        while !self.data.is_empty() {
            let elem_left = self.data.len();
            let steal_counter = steal::get_my_steal_count();
            // TODO: actually use the count, don't just split in two
            if steal_counter > 0 && elem_left > 4096 {
                self.split();
                return;
            }
            // Do some work: Split off and sort piece
            let work_size = std::cmp::min(4096, elem_left);
            let piece = cut_off_left(&mut self.data, work_size);
            piece.sort();
            let buffer = cut_off_left(&mut self.to, work_size);
            let merge = merge::MergeResult::new(piece, buffer, true);
            self.pieces.push(merge);
            // try merging pieces
            self.merge();
        }
        return;
    }
}
impl<'a, T> merge::Task for Mergesort<'a, T>
where
    T: Ord + Sync + Send + Copy,
{
    fn run(&mut self, me: Option<&mut merge::RunTask>) -> bool {
        if me.is_none() {
            return false;
        };
        if self.data.len() < 4096 {
            return false;
        }
        rayon::join(|| self.mergesort(), || me.unwrap());
        return true; // finished
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
