use crate::kmerge_impl;
use crate::rayon;
use crate::steal;
const MIN_WORK_SIZE: usize = 50;
pub fn merge(slices: &[&[usize]], buffer: &mut [usize]) {
    let slice_iters = slices.iter().map(|x| x.iter());
    let mut iter = kmerge_impl::kmerge(slice_iters);

    let mut buffer = buffer.iter_mut();
    loop {
        let steal_counter = steal::get_my_steal_count();
        if steal_counter == 0 || buffer.len() < MIN_WORK_SIZE {
            // Do a part of the work
            for _ in 0..std::cmp::min(MIN_WORK_SIZE, buffer.len()) {
                *buffer.next().unwrap() = *iter.next().unwrap();
            }
            if buffer.len() == 0 {
                return;
            }
        } else {
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
                    rayon_logs::subgraph("merging", buffer.len(), || {
                        merge(&slices, buffer);
                    });
                    // assert!(buffer.windows(2).all(|w| w[0] <= w[1]));
                    return;
                }
                let split = max_slice.len() / steal_counter;
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
                rayon_logs::join(|| spawn(steal_counter - 1, right, b2), || merge(&left, b1));
            }
            spawn(steal_counter + 1 /* me */, slices, buffer);
            return;
        }
    }
}

// The slice has one more item in front you want to take
pub unsafe fn put_back_item<T>(slice: &[T]) -> &[T] {
    // we now get the head by constructing a slice that's one element larger at
    // the front
    let start = slice.get_unchecked(0) as *const T;
    let start = start.offset(-1);
    let len = slice.len() + 1;
    std::slice::from_raw_parts(start, len)
}
pub struct MergeResult<'a, 'b> {
    data: &'a mut [usize], // that's where it starts and should be after it's merged
    buffer: &'b mut [usize], // that's where it temporarily might be
    in_data: bool,         // true if the sorted data is in the data, false if it's buffer
}
impl<'a, 'b> MergeResult<'a, 'b> {
    fn new(data: &'a mut [usize], buffer: &'b mut [usize], in_data: bool) -> MergeResult<'a, 'b> {
        MergeResult {
            data,
            buffer,
            in_data,
        }
    }
    fn location(self: &'a Self) -> &'a [usize] {
        if self.in_data {
            self.data
        } else {
            self.buffer
        }
    }
    fn len(self: &Self) -> usize {
        // assert_eq!(self.data.len(), self.buffer.len());
        return self.data.len();
    }

    fn merge(mut self: &mut Self, other: &MergeResult) -> &mut Self {
        assert!(other.location().windows(2).all(|w| w[0] <= w[1]));
        assert!(self.location().windows(2).all(|w| w[0] <= w[1]));
        let len = self.len() + other.len();
        let start = self.buffer.get_mut(0).unwrap();
        let buffer = unsafe { std::slice::from_raw_parts_mut(start, len) };
        let start = self.data.get_mut(0).unwrap();
        let data = unsafe { std::slice::from_raw_parts_mut(start, len) };
        // be sure that the next block as actually after
        assert_eq!(data[self.len()], other.data[0]);
        assert_eq!(buffer[self.len()], other.buffer[0]);
        if self.in_data {
            // TODO: this could probably by simpler
            two_merge1(self.data, other.location(), buffer);
            self.buffer = buffer;
            self.in_data = false;
            self.data = data;
            assert!(self.buffer.windows(2).all(|w| w[0] <= w[1]));
        } else {
            two_merge1(self.buffer, other.location(), data);
            self.buffer = buffer;
            self.in_data = true;
            self.data = data;
            assert!(self.data.windows(2).all(|w| w[0] <= w[1]));
        }
        self
    }
}
pub type InData = bool;
pub fn two_merge<'a>(
    data: &'a mut [&mut [usize]],
    mut to: &'a mut [usize],
    locations: Vec<InData>,
) -> InData {
    if data.len() < 2 {
        return true; // data is (hopefully) sorted, we didn't do anything
    };
    // assert_eq!(data.iter().map(|x| x.len()).sum::<usize>(), to.len());
    assert_eq!(data.len(), locations.len());
    /*let mut r: Vec<MergeResult<'a>> = */
    let mut x = Vec::new();
    for (v, loc) in data.iter_mut().zip(locations) {
        // this just doesn't seem to work with data.iter().map() because of lifetimes and stuff...
        let (l, r): (&mut [usize], _) = to.split_at_mut(v.len());
        to = r;
        let m = MergeResult::new(v, l, loc);
        x.push(m);
    }
    let mut x: Vec<&mut MergeResult> = x.iter_mut().collect();
    // assert!(x.len() > 0);
    two_merge_prepare(&mut x);
    return x[0].in_data;
}

pub fn two_merge_prepare(data: &mut Vec<&mut MergeResult>) {
    // Result is data[0]
    if data.len() == 1 {
        return;
    };
    let mut x: Vec<&mut MergeResult> = data
        .chunks_mut(2)
        .map(|chunk| {
            if chunk.len() == 1 {
                // chunk.get(0).unwrap()
                // unimplemented!()
                &mut chunk[0]
            } else {
                let (a, b) = chunk.split_at_mut(1);
                let a: &mut MergeResult = &mut a[0];
                let b: &mut MergeResult = &mut b[0];
                // assert_eq!(a.data.len(), a.buffer.len());
                // assert_eq!(b.data.len(), b.buffer.len());
                let x: &mut MergeResult = a.merge(b);
                assert!(x.location().windows(2).all(|w| w[0] <= w[1]));
                // assert_eq!(x.data.len(), x.buffer.len());
                x
            }
        })
        .collect();
    // assert!(x.len() > 0);
    two_merge_prepare(&mut x);
}
// pub fn two_merge(slices: &[&[usize]], buffer: &mut [usize], temp: &mut [usize]) {
//     assert_eq!(slices.iter().map(|x| x.len()).sum::<usize>(), buffer.len());
//     assert_eq!(buffer.len(), temp.len());
//     if slices.len() == 0 {
//         return;
//     }
//     if slices.len() > 2 {
//         let len = slices.len();
//         let (left, right) = slices.split_at(len / 2);
//         let (tleft, tright) = temp.split_at_mut(left.iter().map(|x| x.len()).sum());
//         let (bleft, bright) = buffer.split_at_mut(tleft.len());
//         let steal_counter = steal::get_my_steal_count();
//         if steal_counter > 0 {
//             rayon::join(
//                 || two_merge(left, tleft, bleft),
//                 || two_merge(right, tright, bright),
//             );
//         } else {
//             two_merge(left, tleft, bleft);
//             two_merge(right, tright, bright);
//         }
//         two_merge1(tleft, tright, buffer);
//     } else {
//         if slices.len() == 1 {
//             buffer.copy_from_slice(slices[0]);
//         } else {
//             two_merge1(slices[0], slices[1], buffer);
//         }
//     }
// }
// std::iter::peekable
pub struct Peekable<I: Iterator> {
    iter: I,
    /// Remember a peeked value, even if it was None.
    peeked: Option<Option<I::Item>>,
}
// Itertools::KMergeeBy
pub struct MergeBy<I, J, F>
where
    I: Iterator,
    J: Iterator<Item = I::Item>,
{
    a: Peekable<I>,
    b: Peekable<J>,
    _fused: Option<bool>,
    _cmp: F,
}
pub struct MergeLte;
pub type Merge<I, J> = MergeBy<I, J, MergeLte>;

pub fn two_merge1(a: &[usize], b: &[usize], buffer: &mut [usize]) {
    assert!(a.windows(2).all(|w| w[0] <= w[1]));
    assert!(b.windows(2).all(|w| w[0] <= w[1]));
    assert_eq!(a.len() + b.len(), buffer.len());
    use itertools::Itertools;
    let mut iter = a.iter().merge(b);
    let mut buffer = buffer.iter_mut();
    loop {
        let steal_counter = steal::get_my_steal_count();
        if steal_counter == 0 || buffer.len() < MIN_WORK_SIZE {
            // Do a part of the work
            for _ in 0..std::cmp::min(MIN_WORK_SIZE, buffer.len()) {
                *buffer.next().unwrap() = *iter.next().unwrap();
            }
            if buffer.len() == 0 {
                return; // finished
            }
            assert_eq!(iter.size_hint().0, buffer.len());
        // println!("iter: {}, buffer: {}", iter.size_hint().0, buffer.len());
        } else {
            assert_eq!(iter.size_hint().0, buffer.len());
            // Someone is trying to steal. We need to recover the slices from the merging.
            let mut iter: Merge<std::slice::Iter<usize>, std::slice::Iter<usize>> =
                unsafe { std::mem::transmute(iter) };

            // The rest of the buffer
            let mut a = iter.a.iter.as_slice();
            let mut b = iter.b.iter.as_slice();
            match iter.a.peeked.take() {
                Some(Some(_)) => unsafe { a = put_back_item(a) },
                _ => (),
            }
            match iter.b.peeked.take() {
                Some(Some(_)) => unsafe { b = put_back_item(b) },
                _ => (),
            }
            drop(iter); // after the transmute we probably shouldn't use that thing anymore
            let buffer = buffer.into_slice();
            assert_eq!(a.len() + b.len(), buffer.len());

            fn spawn(steal_counter: usize, a: &[usize], b: &[usize], buffer: &mut [usize]) {
                assert_eq!(a.len() + b.len(), buffer.len());
                let max_slice = if a.len() > b.len() { a } else { b };
                if steal_counter == 1 || max_slice.len() < MIN_WORK_SIZE / 2 {
                    assert!(a.windows(2).all(|w| w[0] <= w[1]));
                    assert!(b.windows(2).all(|w| w[0] <= w[1]));
                    rayon_logs::subgraph("merging", buffer.len(), || {
                        two_merge1(a, b, buffer);
                    });
                    assert!(buffer.windows(2).all(|w| w[0] <= w[1]));
                    return;
                }
                let split = max_slice.len() / steal_counter;
                // the element to split
                let split_elem = max_slice[split];

                // find the splitting points in all splices

                let index_a = split_for_merge(a, &|a, b| a < b, &split_elem);
                let index_b = split_for_merge(b, &|a, b| a < b, &split_elem);
                let (left_a, right_a) = a.split_at(index_a);
                let (left_b, right_b) = b.split_at(index_b);
                let (b1, b2) = buffer.split_at_mut(left_a.len() + left_b.len());
                assert_eq!(left_a.len() + left_b.len(), b1.len());
                assert_eq!(right_a.len() + right_b.len(), b2.len());
                rayon_logs::join(
                    || spawn(steal_counter - 1, right_a, right_b, b2),
                    || two_merge1(left_a, left_b, b1),
                );
            }
            spawn(steal_counter + 1 /* me */, a, b, buffer);
            if !buffer.windows(2).all(|w| {
                if w[0] <= w[1] {
                    true
                } else {
                    println!("Fail: {}, {}", w[0], w[1]);
                    println!("{:?}", buffer);
                    false
                }
            }) {}
            assert!(buffer.windows(2).all(|w| w[0] <= w[1]));
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
