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
    fn split_run(&mut self, steal_counter: usize)
    where
        Self: Sized,
    {
        let mut other = self.split();

        // if steal_counter < 2 {
            rayon::join(
                || {
                    steal::reset_my_steal_count();
                    self.run(None)
                },
                || other.run(None),
            );
        // } else {
        //     rayon::join(
        //         || {
        //             steal::reset_my_steal_count();
        //             self.split_run(steal_counter / 2)
        //         },
        //         || other.split_run(steal_counter / 2),
        //     );
        // }

        self.fuse(other);
        // self.run(None); // Other has one element, we can try to merge that to self
    }
    fn check(&mut self) -> bool
    where
        Self: Sized,
    {
        let steal_counter = steal::get_my_steal_count();
        if steal_counter == 0 {
            return false;
            // return Some(self);
        }
        if !self.can_split() {
            return false;
            // return Some(self);
        }
        self.split_run(steal_counter);
        true
    }
    fn fuse(&mut self, _other: Self) -> ()
    where
        Self: Sized,
    {
        assert!(false);
    }
}
