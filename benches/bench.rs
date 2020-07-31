use adaptive_algorithms::adaptive_bench::*;
use criterion::*;
use mergesort::mergesort;
use rayon::prelude::*;
use rayon_adaptive::adaptive_sort;
extern crate num;
extern crate rand;

// pub fn comparison() {
//     let pool = rayon_logs::ThreadPoolBuilder::new()
//         // .num_threads(4)
//         .steal_callback(move |x| steal::steal(8, x))
//         .build()
//         .unwrap();

//     pool.compare()
//         .attach_algorithm_with_setup(
//             "My mergesort",
//             || V.clone(),
//             |mut v| {
//                 mergesort(&mut v);
//                 verify(&v);
//             },
//         )
//         .attach_algorithm_with_setup(
//             "Rayon par_sort",
//             || V.clone(),
//             |mut v| {
//                 let x: &mut [_] = &mut v;
//                 rayon_logs::prelude::ParallelSliceMut::par_sort(x); // .par_sort();
//                 verify(&v);
//             },
//         )
//         .generate_logs("comparison.html")
//         .expect("Failed saving logs");
//     println!("generated comparison.html");
// }

#[derive(Default, Copy, Clone, Debug)]
struct Tuple {
    left: usize,
    right: usize,
}
impl PartialEq for Tuple {
    fn eq(&self, other: &Tuple) -> bool {
        return self.left == other.left && self.right == other.right;
    }
}
impl Eq for Tuple {}

use std::cmp::Ordering;
impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Tuple) -> Option<Ordering> {
        // self.left.partial_cmp(&other.left)
        self.dist().partial_cmp(&other.dist())
    }
}

impl Tuple {
    fn dist(&self) -> f64 {
        let squared = self.left * self.left + self.right * self.right;
        (squared as f64).sqrt()
    }
}
impl Ord for Tuple {
    fn cmp(&self, other: &Tuple) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}
struct MergeSort<'a, T> {
    original: &'a Vec<T>,
    data: Vec<T>,
}

impl<'a, T: Send + Sync + Copy + Ord> Benchable<'a, T> for MergeSort<'a, T> {
    fn start(&mut self) -> Option<T> {
        *self = MergeSort::new(self.original);
        rayon::join(|| {}, || {});
        mergesort(&mut self.data);
        // assert!(self.data.windows(2).all(|w| w[0] <= w[1]));
        None
    }
    fn name(&self) -> &'static str {
        "Adaptive Mergesort"
    }
}
impl<'a, T: Clone> MergeSort<'a, T> {
    fn new(data: &'a Vec<T>) -> Self {
        MergeSort {
            original: data,
            data: data.clone(),
        }
    }
}
struct RayonAdaptive<'a, T> {
    original: &'a Vec<T>,
    data: Vec<T>,
}

impl<'a, T: Send + Sync + Copy + Ord> Benchable<'a, T> for RayonAdaptive<'a, T> {
    fn start(&mut self) -> Option<T> {
        *self = RayonAdaptive::new(self.original);
        adaptive_sort(&mut self.data);
        // assert!(self.data.windows(2).all(|w| w[0] <= w[1]));
        None
    }
    fn name(&self) -> &'static str {
        "Rayon-Adaptive Mergesort"
    }
}
impl<'a, T: Clone> RayonAdaptive<'a, T> {
    fn new(data: &'a Vec<T>) -> Self {
        RayonAdaptive {
            original: data,
            data: data.clone(),
        }
    }
}

struct Rayon<'a, T> {
    original: &'a Vec<T>,
    data: Vec<T>,
}

impl<'a, T: Send + Sync + Copy + Ord> Benchable<'a, T> for Rayon<'a, T> {
    fn start(&mut self) -> Option<T> {
        *self = Rayon::new(self.original);
        self.data.par_sort();
        // assert!(self.data.windows(2).all(|w| w[0] <= w[1]));
        None
    }
    fn name(&self) -> &'static str {
        "Rayon par_sort()"
    }
}
impl<'a, T: Clone> Rayon<'a, T> {
    fn new(data: &'a Vec<T>) -> Self {
        Rayon {
            original: data,
            data: data.clone(),
        }
    }
}
struct Single<'a, T> {
    original: &'a Vec<T>,
    data: Vec<T>,
}

impl<'a, T: Send + Sync + Copy + Ord> Benchable<'a, T> for Single<'a, T> {
    fn start(&mut self) -> Option<T> {
        *self = Single::new(self.original);
        self.data.sort();
        // assert!(self.data.windows(2).all(|w| w[0] <= w[1]));
        None
    }
    fn name(&self) -> &'static str {
        "Sequential Sort"
    }
}
impl<'a, T: Clone> Single<'a, T> {
    fn new(data: &'a Vec<T>) -> Self {
        Single {
            original: data,
            data: data.clone(),
        }
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("MergeSorting");
    group.warm_up_time(std::time::Duration::new(1, 0));
    group.measurement_time(std::time::Duration::new(1, 0));
    group.sample_size(10);
    // let v_20: Vec<u32> = std::iter::repeat_with(rand::random)
    //     .take(3usize.pow(13))
    //     // .map(|x: u32| x % 1_000_000)
    //     .collect();
    // let v_21: Vec<u32> = std::iter::repeat_with(rand::random)
    //     .take(2usize.pow(21))
    //     // .map(|x: u32| x % 1_000_000)
    //     .collect();

    let v: Vec<Tuple> = std::iter::repeat_with(rand::random)
        .take(3usize.pow(12))
        .enumerate()
        .map(|(x, y): (usize, usize)| Tuple { left: y, right: x })
        .collect();

    let cpus: Vec<usize> = vec![1, 2, 3, 4, 8, 16, 24, 32]
        .iter()
        .filter(|&&i| i <= num_cpus::get())
        .cloned()
        .collect();

    let mut tests: Vec<TestConfig<_>> = vec![];

    let data = vec![&v /*&v_21*/];
    for v in &data {
        let test = Single::new(&v);
        let x = TestConfig::new(v.len(), 1, None, test);
        tests.push(x);
        for i in &cpus {
            for s in vec![0, 4, 6, 8] {
                let test = MergeSort::new(&v);
                let x = TestConfig::new(v.len(), *i, Some(s), test);
                tests.push(x);
            }
            let test = RayonAdaptive::new(&v);
            let x = TestConfig::new(v.len(), *i, None, test);
            tests.push(x);

            let test = Rayon::new(&v);
            let x = TestConfig::new(v.len(), *i, None, test);
            tests.push(x);
        }
    }

    let mut t = Tester::new(tests, group, None);
    t.run();

    // group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
