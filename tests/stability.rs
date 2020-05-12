use mergesort::mergesort;
use mergesort::rayon;
pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    // test for stability with a Tuple where we only sort by the first element, then test if the
    // second elements stayed in the same order
    let mut v: Vec<Tuple> = std::iter::repeat_with(rand::random)
        .take(2usize.pow(20))
        .enumerate()
        .map(|(x, y): (usize, usize)| Tuple {
            left: y % 10,
            right: x,
        })
        .collect();
    let pool = rayon::get_thread_pool();
    let _  = pool.install(|| mergesort(&mut v));
    assert!(v.windows(2).all(|w| w[0] <= w[1]));
    assert!(v
        .windows(2)
        .all(|w| w[0] != w[1] || w[0].right <= w[1].right));
    Ok(())
}

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
