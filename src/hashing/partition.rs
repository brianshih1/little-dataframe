use crate::core::POOL;

#[inline]
// Got this from Polars: https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/hashing/partition.rs#L134
/// For partitions that are a power of 2 we can use a bitshift instead of a modulo.
pub fn this_partition(hash: u64, thread_no: u64, n_partitions: u64) -> bool {
    debug_assert!(n_partitions.is_power_of_two());
    // n % 2^i = n & (2^i - 1)
    (hash & n_partitions.wrapping_sub(1)) == thread_no
}

// From Polars: https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/utils/mod.rs#L31
pub fn _set_partition_size() -> usize {
    let mut n_partitions = POOL.current_num_threads();
    if n_partitions == 1 {
        return 1;
    }
    // set n_partitions to closest 2^n size
    loop {
        if n_partitions.is_power_of_two() {
            break;
        } else {
            n_partitions -= 1;
        }
    }
    n_partitions
}
