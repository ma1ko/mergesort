use crate::crossbeam::{Backoff, CachePadded};
use num_cpus;
use std::sync::atomic::{AtomicUsize, Ordering};
lazy_static! {
    static ref NUM_THREADS: usize = num_cpus::get();
    static ref V: Vec<CachePadded<AtomicUsize>> = (0..*NUM_THREADS)
        .map(|_| CachePadded::new(AtomicUsize::new(0)))
        .collect();
}
// pub fn active() {
//     let thread_index = rayon::current_thread_index().unwrap();
//     V[thread_index].1.store(true, Ordering::Relaxed);
// }
// pub fn inactive() {
//     let thread_index = rayon::current_thread_index().unwrap();
//     V[thread_index].1.store(false, Ordering::Relaxed);
// }

pub fn steal(backoffs: usize, victim: usize) -> Option<()> {
    let thread_index = rayon::current_thread_index().unwrap();
    let thread_index = 1 << thread_index;
    V[victim].fetch_or(thread_index, Ordering::Relaxed);
    //V[victim].fetch_add(1, Ordering::Relaxed);

    let backoff = Backoff::new();
    let mut c: usize;
    for _ in 0..backoffs {
        backoff.spin(); // spin or snooze()?

        // wait until the victim has taken the value, check regularly
        c = V[victim].load(Ordering::Relaxed);
        if c == 0 {
            return Some(());
        }
    }

    V[victim].fetch_and(!thread_index, Ordering::Relaxed);
    //let i = V[victim].fetch_sub(1, Ordering::Relaxed);

    //let _ = V[victim].compare_exchange_weak(c, c - 1, Ordering::Relaxed, Ordering::Relaxed);

    None
}
pub fn get_my_steal_count() -> usize {
    let thread_index = rayon::current_thread_index().unwrap();
    let steal_counter = V[thread_index].load(Ordering::Relaxed);
    let steal_counter = steal_counter.count_ones() as usize;
    let steal_counter = std::cmp::min(steal_counter, *NUM_THREADS - 1);
    steal_counter
}
pub fn reset_my_steal_count() {
    let thread_index = rayon::current_thread_index().unwrap();
    V[thread_index].store(0, Ordering::Relaxed);
}
