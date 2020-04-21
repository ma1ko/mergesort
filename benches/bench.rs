use criterion::Bencher;
use criterion::BenchmarkGroup;
use criterion::*;
use mergesort::{mergesort, steal};
use rayon::prelude::*;
use rayon_adaptive::adaptive_sort;
#[macro_use]
extern crate lazy_static;
lazy_static! {
    static ref V: Vec<u32> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(21))
        .map(|x: u32| x % 1_000_000)
        .collect();
    static ref CHECKSUM: u32 = V.iter().sum::<u32>();
}

fn verify(numbers: &Vec<u32>) {
    assert_eq!(numbers.iter().sum::<u32>(), *CHECKSUM);
    assert!(numbers.windows(2).all(|w| w[0] <= w[1]));
}

pub fn adaptive(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for steal_counter in &[1, 2, 4, 6, 8, 10] {
        for size in &[2, 4, 6, 8, 12, 16, 20, 24, 32] {
            group.bench_with_input(
                BenchmarkId::new(format!("Adaptive_{}", steal_counter), size),
                &size,
                |b, _| {
                    let pool = rayon::ThreadPoolBuilder::new()
                        .num_threads(*size)
                        .steal_callback(move |x| steal::steal(*steal_counter, x))
                        .build()
                        .unwrap();

                    b.iter_batched(
                        || V.clone(),
                        |mut numbers| {
                            pool.install(|| {
                                mergesort(&mut numbers);
                                verify(&numbers);
                            });
                        },
                        BatchSize::SmallInput,
                    );
                },
            );
        }
    }
}
pub fn rayon_adaptive(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for size in &[2, 4, 6, 8, 12, 16, 20, 24, 32] {
        group.bench_with_input(
            BenchmarkId::new("rayon-adaptive", size),
            &size,
            |b, &size| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(*size)
                    .build()
                    .unwrap();

                b.iter_batched(
                    || V.clone(),
                    |mut numbers| {
                        pool.install(|| {
                            adaptive_sort(&mut numbers);
                            verify(&numbers);
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}
pub fn comparison() {
    let pool = rayon_logs::ThreadPoolBuilder::new()
        // .num_threads(4)
        .steal_callback(move |x| steal::steal(8, x))
        .build()
        .unwrap();

    pool.compare()
        .attach_algorithm_with_setup(
            "My mergesort",
            || V.clone(),
            |mut v| {
                mergesort(&mut v);
                verify(&v);
            },
        )
        .attach_algorithm_with_setup(
            "Rayon par_sort",
            || V.clone(),
            |mut v| {
                let x: &mut [_] = &mut v;
                rayon_logs::prelude::ParallelSliceMut::par_sort(x); // .par_sort();
                verify(&v);
            },
        )
        .generate_logs("comparison.html")
        .expect("Failed saving logs");
    println!("generated comparison.html");
}

pub fn iterator(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for size in &[2, 4, 6, 8, 12, 16, 20, 24, 32] {
        group.bench_with_input(BenchmarkId::new("Iterator", size), &size, |b, &size| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(*size)
                .build()
                .unwrap();

            b.iter_batched(
                || V.clone(),
                |mut numbers| {
                    /*let (_, log) =*/
                    pool.install(|| {
                        numbers.par_sort();
                        verify(&numbers);
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
}
pub fn single(b: &mut Bencher) {
    b.iter_batched(
        || V.clone(),
        |mut numbers| {
            numbers.sort();
            verify(&numbers);
        },
        BatchSize::SmallInput,
    );
}

fn bench(c: &mut Criterion) {
    println!("{}", V[0]); // to init lazy_static
    let mut group = c.benchmark_group("MergeSorting");
    group.measurement_time(std::time::Duration::new(3, 0));
    group.warm_up_time(std::time::Duration::new(1, 0));
    group.sample_size(10);

    adaptive(&mut group);
    rayon_adaptive(&mut group);
    iterator(&mut group);
    // comparison();

    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
