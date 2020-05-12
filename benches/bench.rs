use criterion::BenchmarkGroup;
use criterion::*;
use mergesort::{mergesort, steal};
use rayon::prelude::*;
#[macro_use]
extern crate lazy_static;
extern crate num;
extern crate rand;
lazy_static! {
    static ref V: Vec<u32> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20) - 10000)
        .map(|x: u32| x % 1_000_000)
        .collect();
    static ref CHECKSUM: u32 = V.iter().sum::<u32>();
}

type Group<'a> = BenchmarkGroup<'a, criterion::measurement::WallTime>;
struct Tester<'a, T: 'a> {
    numbers: Vec<T>,
    checksum: T,
    tests: Vec<Box<dyn Test<T>>>,
    group: Group<'a>,
}
impl<'a, T> Tester<'a, T>
where
    T: num::Integer + std::fmt::Debug + std::iter::Sum<T> + Copy + Default,
{
    fn verify(checksum: T, numbers: Vec<T>) {
        assert_eq!(numbers.iter().cloned().sum::<T>(), checksum);
        assert!(numbers.windows(2).all(|w| w[0] <= w[1]));
    }
    fn new(numbers: Vec<T>, tests: Vec<Box<dyn Test<T>>>, group: Group<'a>) -> Self {
        let checksum: T = numbers.iter().cloned().sum();
        Tester {
            numbers,
            checksum,
            tests,
            group,
        }
    }
    fn run(&mut self) {
        for test in &self.tests {
            let numbers = self.numbers.clone();
            // let mut group = self.group.clone();;
            let group = &mut self.group;
            let checksum = self.checksum;
            group.bench_with_input(
                test.id(),
                // BenchmarkId::new(format!("Adaptive_{}", 2), size),
                &numbers,
                |b, numbers| {
                    b.iter_batched(
                        || numbers.clone(),
                        |mut numbers| {
                            test.run(&mut numbers);
                            Tester::verify(checksum, numbers);
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}

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

trait Test<T> {
    fn run(&self, numbers: &mut Vec<T>) -> ();
    fn name(&self) -> &'static str;
    fn id(&self) -> BenchmarkId;
}

struct Adaptive {
    t: rayon::ThreadPool,
    num_threads: usize,
    steal_counter: usize,
}
impl<T> Test<T> for Adaptive
where
    T: num::Integer + std::fmt::Debug + Copy + Sync + Send,
{
    fn run(&self, mut numbers: &mut Vec<T>) {
        self.t.install(|| mergesort(&mut numbers));
    }
    fn name(&self) -> &'static str {
        "Adaptive"
    }
    fn id(&self) -> BenchmarkId {
        BenchmarkId::new(format!("Adaptive_{}", self.steal_counter), self.num_threads)
    }
}
impl Adaptive {
    fn new(num_threads: usize, steal_counter: usize) -> Self {
        Adaptive {
            t: rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .steal_callback(move |x| steal::steal(steal_counter, x))
                .build()
                .unwrap(),
            num_threads,
            steal_counter,
        }
    }
}
struct RayonAdaptive {
    t: rayon::ThreadPool,
    num_threads: usize,
}
impl<T> Test<T> for RayonAdaptive
where
    T: num::Integer + std::fmt::Debug + Copy + Sync + Send,
{
    fn run(&self, numbers: &mut Vec<T>) {
        self.t.install(|| rayon_adaptive::adaptive_sort(numbers));
    }
    fn name(&self) -> &'static str {
        "Adaptive"
    }
    fn id(&self) -> BenchmarkId {
        BenchmarkId::new("Rayon-Adaptive", self.num_threads)
    }
}
impl RayonAdaptive {
    fn new(num_threads: usize) -> Self {
        RayonAdaptive {
            t: rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .unwrap(),
            num_threads,
        }
    }
}

struct Rayon {
    t: rayon::ThreadPool,
    num_threads: usize,
}
impl<T> Test<T> for Rayon
where
    T: num::Integer + std::fmt::Debug + Copy + Sync + Send,
{
    fn run(&self, numbers: &mut Vec<T>) {
        self.t.install(|| numbers.par_sort());
    }
    fn name(&self) -> &'static str {
        "Rayon"
    }
    fn id(&self) -> BenchmarkId {
        BenchmarkId::new("Rayon", self.num_threads)
    }
}
impl Rayon {
    fn new(num_threads: usize) -> Self {
        Rayon {
            t: rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads)
                .build()
                .unwrap(),
            num_threads,
        }
    }
}
struct Single {
    num_threads: usize,
}
impl<T> Test<T> for Single
where
    T: num::Integer + std::fmt::Debug + Copy + Sync + Send,
{
    fn run(&self, numbers: &mut Vec<T>) {
        numbers.sort();
    }
    fn name(&self) -> &'static str {
        "Single"
    }
    fn id(&self) -> BenchmarkId {
        BenchmarkId::new("Single", self.num_threads)
    }
}
impl Single {
    fn new(num_threads: usize) -> Self {
        Single { num_threads }
    }
}

fn bench(c: &mut Criterion) {
    println!("{}", V[0]); // to init lazy_static
    let mut group = c.benchmark_group("MergeSorting");
    group.warm_up_time(std::time::Duration::new(1, 0));
    group.sample_size(10);
    group.nresamples(10);
    /*
    let mut v: Vec<u32> = (0..1 << 20).into_iter().collect();
    let mut rng = rand::thread_rng();
    use rand::seq::SliceRandom;
    v.shuffle(&mut rng);
    */

    let v_20: Vec<u32> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        // .map(|x: u32| x % 1_000_000)
        .collect();
    // let v_21: Vec<u32> = std::iter::repeat_with(rand::random)
    //     .take(2usize.pow(21))
    //     .map(|x: u32| x % 1_000_000)
    //     .collect();

    let cpus: Vec<usize> = vec![1, 2, 3, 4, 8, 16, 24, 32]
        .iter()
        .filter(|&&i| i <= num_cpus::get())
        .cloned()
        .collect();

    let mut test: Vec<Box<dyn Test<u32>>> = vec![];
    for i in &cpus {
        for s in vec![6, 8] {
            let x: Box<dyn Test<u32>> = Box::new(Adaptive::new(*i, s));
            test.push(x);
        }
        let x: Box<dyn Test<u32>> = Box::new(RayonAdaptive::new(*i));
        test.push(x);
        let x: Box<dyn Test<u32>> = Box::new(Rayon::new(*i));
        test.push(x);
    }
    let x: Box<dyn Test<u32>> = Box::new(Single::new(1));
    test.push(x);
    let mut t = Tester::new(v_20, test, group);
    // let mut t = Tester::new(v_21, test, group);
    t.run();

    // group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
