use criterion::Bencher;
use criterion::BenchmarkGroup;
use criterion::*;
use mergesort::{mergesort, steal};
// use rayon_logs::prelude::*;
use rayon::prelude::*;
#[macro_use]
extern crate lazy_static;
lazy_static! {
    static ref V: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(1_000_000)
        .map(|x: usize| x % 1_000_000)
        .collect();
    static ref checksum: usize = V.iter().sum();
}

fn verify(numbers: &Vec<usize>) {
    assert!(numbers.windows(2).all(|w| w[0] <= w[1]));
    assert_eq!(numbers.iter().sum::<usize>(), *checksum);
}

pub fn adaptive(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for size in 1..5 {
        group.bench_with_input(BenchmarkId::new("Adaptive", size), &size, |b, &size| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(size)
                .steal_callback(move |x| steal(15, x))
                .build()
                .unwrap();

            b.iter_batched(
                || V.clone(),
                |mut numbers| {
                    /*let (_, log) =*/
                    pool.install(|| {
                        mergesort(&mut numbers);
                        verify(&numbers);
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
}

pub fn iterator(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for size in 1..5 {
        group.bench_with_input(BenchmarkId::new("Iterator", size), &size, |b, &size| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(size)
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
    println!("{}", V[0]);
    let mut group = c.benchmark_group("MergeSorting");
    group.measurement_time(std::time::Duration::new(3, 0));
    group.warm_up_time(std::time::Duration::new(1, 0));
    group.sample_size(10);

    adaptive(&mut group);
    group.bench_function("single", |mut b: &mut Bencher| {
        single(&mut b);
    });

    /*
    group.bench_function("iterator", |mut b: &mut Bencher| {
        iterator(&mut b);
    });
    */
    iterator(&mut group);

    /*
    group.bench_function("perfect split", |b| {
        perfect_split(b);
    });
    */

    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
