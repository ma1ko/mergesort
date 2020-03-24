use std::mem;
use std::ptr;
pub fn insertion_sort<F, T>(v: &mut [T], is_less: &F)
where
    F: Fn(&T, &T) -> bool,
{
    if v.len() >= 2 {
        for i in (0..v.len() - 1).rev() {
            insert_head(&mut v[i..], is_less);
        }
    }
}

fn insert_head<T, F>(v: &mut [T], is_less: &F)
where
    F: Fn(&T, &T) -> bool,
{
    if v.len() >= 2 && is_less(&v[1], &v[0]) {
        unsafe {
            let mut tmp = NoDrop {
                value: Some(ptr::read(&v[0])),
            };

            // Intermediate state of the insertion process is always tracked by `hole`, which
            // serves two purposes:
            // 1. Protects integrity of `v` from panics in `is_less`.
            // 2. Fills the remaining hole in `v` in the end.
            //
            // Panic safety:
            //
            // If `is_less` panics at any point during the process, `hole` will get dropped and
            // fill the hole in `v` with `tmp`, thus ensuring that `v` still holds every object it
            // initially held exactly once.
            let mut hole = InsertionHole {
                src: tmp.value.as_mut().unwrap(),
                dest: &mut v[1],
            };
            ptr::copy_nonoverlapping(&v[1], &mut v[0], 1);

            for i in 2..v.len() {
                if !is_less(&v[i], tmp.value.as_ref().unwrap()) {
                    break;
                }
                ptr::copy_nonoverlapping(&v[i], &mut v[i - 1], 1);
                hole.dest = &mut v[i];
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole in `v`.
        }
    }

    // Holds a value, but never drops it.
    struct NoDrop<T> {
        value: Option<T>,
    }

    impl<T> Drop for NoDrop<T> {
        fn drop(&mut self) {
            mem::forget(self.value.take());
        }
    }

    // When dropped, copies from `src` into `dest`.
    struct InsertionHole<T> {
        src: *mut T,
        dest: *mut T,
    }

    impl<T> Drop for InsertionHole<T> {
        fn drop(&mut self) {
            unsafe {
                ptr::copy_nonoverlapping(self.src, self.dest, 1);
            }
        }
    }
}
