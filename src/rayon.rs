use crate::steal;

#[cfg(feature = "logs")]
pub fn get_thread_pool() -> rayon_logs::ThreadPool {
    rayon_logs::ThreadPoolBuilder::new()
        .steal_callback(|x| steal::steal(8, x))
        .build()
        .unwrap()
}
#[cfg(not(feature = "logs"))]
pub fn get_thread_pool() -> rayon::ThreadPool {
    rayon::ThreadPoolBuilder::new()
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
    #[cfg(feature = "logs")]
    return rayon_logs::join(
        {
            steal::reset_my_steal_count();
            oper_a
        },
        || oper_b(),
    );
    #[cfg(not(feature = "logs"))]
    return rayon::join(
        {
            steal::reset_my_steal_count();
            oper_a
        },
        || oper_b(),
    );
}

#[allow(unused)]
pub fn subgraph<OP, R>(work_type: &'static str, work_amount: usize, op: OP) -> R
where
    OP: FnOnce() -> R,
{
    #[cfg(feature = "logs")]
    return rayon_logs::subgraph(work_type, work_amount, op);
    #[cfg(not(feature = "logs"))]
    return op();
}
