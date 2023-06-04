use std::{
    collections::hash_map::RandomState,
    hash::{Hash, Hasher},
};

use hashbrown::{hash_map::RawEntryMut, HashMap};
use rayon::{
    current_thread_index,
    prelude::{IntoParallelIterator, ParallelIterator},
};

use crate::{core::POOL, series::Series};

use super::DataFrame;

impl DataFrame {
    fn inner_join<I, S>(&self, df2: &DataFrame, select_1: I, select_2: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // TODO: Type checking
        let foo = self.select_series(select_1);
        todo!()
    }

    fn hash_series(series: Vec<Series>) -> Vec<u64> {
        if series.len() == 0 {
            panic!("empty series vec")
        }
        let first = series.first().unwrap();
        let mut buf = Vec::with_capacity(series.len());
        first.vec_hash(RandomState::default(), &mut buf);
        todo!()
    }
}

pub struct IdxHash {
    pub idx: usize,
    pub hash: u64,
}

impl Hash for IdxHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash)
    }
}

// Rows corresponding to the same row value are stored in the same HashMap entry.
// Therefore, the hash and one of the indices for each unique row is stored instead of just
// the hash.
pub fn build_probe_table(
    hashes: &Vec<Vec<u64>>,
    dataframe: &DataFrame,
) -> Vec<HashMap<IdxHash, Vec<usize>>> {
    let num_threads = POOL.current_num_threads();

    for i in 0..num_threads {}
    POOL.install(|| {
        (0..num_threads).into_par_iter().map(|thread_no| {
            let mut offset = 0;
            let mut hashmap = HashMap::<IdxHash, Vec<usize>>::new(); // TODO: What capacity should I use?

            for hashes in hashes {
                hashes.iter().enumerate().for_each(|(idx, hash)| {
                    if hash % thread_no as u64 == 0 {
                        let idx = offset + idx;
                        let entry = hashmap.raw_entry_mut().from_hash(*hash, |idx_hash| {
                            idx_hash.hash == *hash && {
                                let entry_idx = idx_hash.idx;
                                compare_df_row(dataframe, idx, entry_idx)
                            }
                        });
                        match entry {
                            RawEntryMut::Occupied(mut occupied) => {
                                let key_value = occupied.get_key_value_mut();
                                key_value.1.push(idx);
                            }
                            RawEntryMut::Vacant(entry) => {
                                entry.insert_hashed_nocheck(
                                    *hash,
                                    IdxHash {
                                        idx: idx,
                                        hash: *hash,
                                    },
                                    vec![idx],
                                );
                            }
                        };
                    }
                });
                offset += hashes.len();
            }

            hashmap
        })
    })
    .collect()
}

fn compare_df_row(df: &DataFrame, idx1: usize, idx2: usize) -> bool {
    for series in &df.columns {
        let is_equal = unsafe { series.equal_element(idx1, series, idx2) };
        if !is_equal {
            return false;
        }
    }
    true
}
