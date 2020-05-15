use crate::steal;

// pub trait Task<T>: Send + Sync {
//     // run self *and* me, or return false if you can't
//     fn run(&mut self, parent: Option<&mut dyn Task>) -> ();
//     fn split(&mut self) -> Box<dyn Task>;
//     fn can_split(&self) -> bool;
//     fn split_run(&mut self, steal_counter: usize)
//     {
//         let mut other = self.split();

//         // if steal_counter < 2 {
//         rayon::join(
//             || {
//                 steal::reset_my_steal_count();
//                 self.run(None)
//             },
//             || other.run(None),
//         );
//         // } else {
//         //     rayon::join(
//         //         || {
//         //             steal::reset_my_steal_count();
//         //             self.split_run(steal_counter / 2)
//         //         },
//         //         || other.split_run(steal_counter / 2),
//         //     );
//         // }

//         self.fuse(other);
//         // self.run(None); // Other has one element, we can try to merge that to self
//     }
//     fn check(&mut self) -> bool
//     {
//         let steal_counter = steal::get_my_steal_count();
//         if steal_counter == 0 {
//             return false;
//             // return Some(self);
//         }
//         if !self.can_split() {
//             return false;
//             // return Some(self);
//         }
//         self.split_run(steal_counter);
//         true
//     }
//     fn get_result(&self) -> T;
//     fn fuse(&mut self, _other: T) -> ()
//     {
//         assert!(false);
//     }
// }
//
// if you can't use None because of typing errors, use Option<Dummy> = None
#[derive(Copy, Clone)]
pub struct Dummy;
pub const NOTHING: Option<&mut Dummy> = None;

pub type NoTask<'a> = Option<&'a mut Dummy>;
impl Task for Dummy {
    fn step(&mut self) {
        assert!(false);
    }
    fn can_split(&self) -> bool {
        assert!(false);
        false
    }
    fn split(&mut self) -> Self {
        assert!(false);
        Dummy {}
    }
    fn is_finished(&self) -> bool {
        assert!(false);
        true
    }
}
pub trait Task: Send + Sync + Sized {
    // run self *and* me, or return false if you can't
    fn run_(&mut self) {
        self.run(NOTHING)
    }
    fn run(&mut self, mut f: Option<&mut impl Task>) {
        while !self.is_finished() {
            let steal_counter = steal::get_my_steal_count();
            if steal_counter != 0 && self.can_split() {
                self.split_run(steal_counter, f.take());
                continue;
            }
            self.step();
        }
    }
    fn step(&mut self);
    fn split_run(&mut self, steal_counter: usize, mut f: Option<&mut impl Task>) {
        // run the parent task
        if let Some(f) = f.take() {
            if f.can_split() {
                let mut other = f.split();
                rayon::join(
                    || other.run_(),
                    || {
                        self.run_();
                        f.run_()
                    },
                );
                f.fuse(other);
                return;
            }
        }

        let mut other: Self = self.split();
        // if steal_counter < 2 {
            rayon::join(
                || {
                    steal::reset_my_steal_count();
                    self.run_()
                },
                || other.run_(),
            );
            self.fuse(other);
        // } else {
        //     rayon::join(
        //         || self.split_run(steal_counter / 2, NOTHING),
        //         || other.split_run(steal_counter / 2, NOTHING),
        //     );
        // }

        // self.fuse(other);
        // self.run(None); // Other has one element, we can try to merge that to self
    }
    // fn check(&mut self, f: Option<impl Task>) -> bool {}
    fn can_split(&self) -> bool;
    fn is_finished(&self) -> bool;
    fn split(&mut self) -> Self;
    fn fuse(&mut self, _other: Self) {}
}
