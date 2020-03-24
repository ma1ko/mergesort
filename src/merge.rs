use crate::kmerge_impl;
use crate::steal;
const MIN_WORK_SIZE: usize = 5000;
pub fn merge(slices: Vec<&[usize]>, buffer: &mut [usize]) {
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
                        merge(slices, buffer);
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
