use std::collections::hash_map::RandomState;

pub mod implementations;

pub trait VecHash {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>);

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]);
}
