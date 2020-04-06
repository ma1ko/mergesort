use criterion::Bencher;
use criterion::BenchmarkGroup;
use criterion::*;
use mergesort::merge::two_merge1;
use mergesort::{mergesort, steal};
// use rayon_logs::prelude::*;
use rayon::prelude::*;
#[macro_use]
extern crate lazy_static;
use itertools;
lazy_static! {
    static ref V: Vec<usize> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        .map(|x: usize| x % 1_000_000)
        .collect();
    static ref checksum: usize = V.iter().sum();
}

fn verify(numbers: &Vec<usize>) {
    assert_eq!(numbers.iter().sum::<usize>(), *checksum);
    assert!(numbers.windows(2).all(|w| w[0] <= w[1]));
}
pub fn merge_speed_test(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    let mut v = V.clone();
    v.sort();
    v.reverse();
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .steal_callback(move |x| steal::steal(10, x))
        .build()
        .unwrap();
    for size in 2..10 {
        let mut res = Vec::new();
        res.resize(V.len(), 0);

        group.bench_with_input(BenchmarkId::new("Merging_mine", size), &size, |b, &size| {
            b.iter_batched(
                || (v.clone(), res.clone()),
                |(mut v, mut buffer)| {
                    let len = v.len();
                    let mut chunks: Vec<&mut [usize]> = v.chunks_mut(len / size).collect();
                    // let mut buffer = res.clone();

                    assert_eq!(res.len(), buffer.len());

                    // res.iter_mut()
                    //     .zip(itertools::kmerge(chunks))
                    //     .for_each(|(mut r, v)| *r = *v);
                    let mut locations = Vec::new();
                    chunks.iter().for_each(|x| locations.push(true));
                    chunks.iter_mut().for_each(|x| x.sort());

                    let chunks: &mut [&mut [usize]] = &mut chunks;
                    // let x = pool.install(|| two_merge1(chunks, &mut buffer, locations));
                    // if x {
                    //     buffer.iter_mut().zip(v).for_each(|(t, b)| *t = b);
                    // }
                    verify(&buffer.to_vec());

                    // let res = pool.install(|| {
                    //     let mut locations = Vec::new();
                    //     chunks.iter().for_each(locations.push(true));
                    //     two_merge(&chunks, &mut res, locations);
                    //     res
                    // });
                    // verify(&res);
                },
                BatchSize::SmallInput,
            );
        });
    }
}
pub fn adaptive(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for size in 1..5 {
        //&[2, 4, 6, 8, 10, 12, 15, 20, 25, 30] {
        group.bench_with_input(BenchmarkId::new("Adaptive", size), &size, |b, &size| {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(size)
                .steal_callback(move |x| steal::steal(10, x))
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
pub fn rayon_adaptive(group: &mut BenchmarkGroup<criterion::measurement::WallTime>) {
    for size in 1..5 {
        // &[2, 4, 6, 8, 10, 15, 20, 25, 30] {
        group.bench_with_input(
            BenchmarkId::new("Really Adaptive", size),
            &size,
            |b, &size| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(size)
                    .build()
                    .unwrap();

                b.iter_batched(
                    || V.clone(),
                    |mut numbers| {
                        pool.install(|| {
                            //adaptive_sort(&mut numbers);
                            verify(&numbers);
                        });
                    },
                    BatchSize::SmallInput,
                );
            },
        );
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

    //rayon_adaptive(&mut group);
    adaptive(&mut group);
    // group.bench_function("single", |mut b: &mut Bencher| {
    //     single(&mut b);
    // });

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
    // merge_speed_test(&mut group);

    group.finish();
}
criterion_group!(benches, bench);
criterion_main!(benches);
