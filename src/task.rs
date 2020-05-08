use crate::steal;

pub trait Task: Send + Sync {
    // run self *and* me, or return false if you can't
    fn run(&mut self, parent: Option<&mut dyn Task>) -> ()
    where
        Self: Sized;
    fn split(&mut self) -> Self
    where
        Self: Sized;
    fn can_split(&self) -> bool;
    fn split_or_run(&mut self, _steal_counter: Option<usize>) -> ()
    where
        Self: Sized,
    {
        let mut other = self.split();
        steal::reset_my_steal_count();
        // if steal_counter.unwrap_or(0) < 2
        /* || elem_left < 2 * *MIN_SPLIT_SIZE */
        // {
            rayon::join(|| self.run(None), || other.run(None));
        // } else {
            // rayon::join(|| self.split(), || other.split());
        // }

        self.fuse(other);
        // self.run(None); // Other has one element, we can try to merge that to self
    }
    fn fuse(&mut self, _other: Self) -> ()
    where
        Self: Sized,
    {
        assert!(false);
    }
}
