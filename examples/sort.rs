use adaptive_algorithms::*;
use mergesort::*;
use rand::prelude::*;
pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    fn random_vec(size: usize) -> Vec<u64> {
        let mut v: Vec<u64> = (0..(size as u64)).collect();
        v.shuffle(&mut thread_rng());
        v
    }
    let mut v = random_vec(1000000);

    let checksum: u64 = v.iter().cloned().sum();
    println!("Finished generating");

    #[cfg(feature = "logs")]
    {
        let pool = rayon::get_thread_pool();
        let (_, log) = pool.logging_install(|| mergesort(&mut v));
        println!("Saving log");
        log.save("test").expect("failed saving log");
        // println!("Saving svg");
        // log.save_svg("test.svg").expect("failed saving svg");
    }
    #[cfg(not(feature = "logs"))]
    {
        let pool = rayon::get_thread_pool();
        let _ = pool.install(|| mergesort(&mut v));
    }
    assert_eq!(checksum, v.iter().sum::<u64>(), "failed merging");
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    #[cfg(feature = "statistics")]
    adaptive_algorithms::task::print_statistics();
    Ok(())
}
