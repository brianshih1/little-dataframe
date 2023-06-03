use std::collections::hash_map::RandomState;

use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::{core::POOL, dataframe::DataFrame, series::Series};

pub mod implementations;

pub trait VecHash {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>);

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]);
}

pub fn series_to_hashes(series_list: &[Series]) -> Vec<u64> {
    if series_list.len() == 0 {
        panic!("hashing 0 series")
    }
    let first = series_list.first().unwrap();
    let mut hashes = Vec::with_capacity(first.len());
    let hasher = RandomState::default();
    first.vec_hash(hasher.clone(), &mut hashes);
    for i in 1..series_list.len() {
        let series = &series_list[i];
        series.vec_hash_combine(hasher.clone(), &mut hashes);
    }
    hashes
}

pub fn hash_dataframes(frames: &[DataFrame]) -> Vec<Vec<u64>> {
    POOL.install(|| frames.par_iter().map(|df| series_to_hashes(&df.columns)))
        .collect()
}
