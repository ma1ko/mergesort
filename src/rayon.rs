use crate::steal;
pub const NUM_THREADS: usize = 4;
pub fn get_thread_pool() -> rayon_logs::ThreadPool {
    rayon_logs::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .steal_callback(|x| steal::steal(8, x))
        .build()
        .unwrap()
}

// quick abstraction that allow to switch easily between rayon and rayon_logs
pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    rayon::join(oper_a, oper_b)
}

pub fn subgraph<OP, R>(work_type: &'static str, work_amount: usize, op: OP) -> R
where
    OP: FnOnce() -> R,
{
    // rayon_logs::subgraph(work_type, work_amount, op)
    op()
}
