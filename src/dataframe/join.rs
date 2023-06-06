use std::{
    collections::hash_map::RandomState,
    hash::{Hash, Hasher},
};

use hashbrown::{hash_map::RawEntryMut, HashMap};
use rayon::{
    current_thread_index,
    prelude::{IntoParallelIterator, ParallelIterator},
};
use rayon::{iter::IndexedParallelIterator, prelude::IntoParallelRefIterator};

use crate::{
    chunked_array::builder::NewFrom,
    core::POOL,
    dataframe::utils::split_df,
    hashing::hash_dataframes,
    series::{self, Series},
};

use super::DataFrame;

impl DataFrame {
    pub fn inner_join<I, S>(&self, select_1: I, df2: &DataFrame, select_2: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // TODO: Type checking
        let series1 = self.select_series(select_1);
        let series2 = df2.select_series(select_2);
        let df1 = DataFrame::new_no_checks(series1);
        let df2 = DataFrame::new_no_checks(series2);

        let (df1_indices, df2_indices) = compute_inner_join_indices(&df1, &df2);
        let df1 = df1.create_df_from_slice(&df1_indices);

        let df2 = df2.remove_columns(
            &df2.columns
                .iter()
                .map(|series| series.name())
                .collect::<Vec<&str>>(),
        );
        let df2 = df2.create_df_from_slice(&df2_indices);
        combine_dataframes(&df1, &df2)
    }

    pub fn create_df_from_slice(&self, indices: &[usize]) -> Self {
        // TODO: Perform optimizations around taking indices like Polars:
        // https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/frame/hash_join/mod.rs#L344
        let columns = &self.columns;
        let series = POOL
            .install(|| {
                columns
                    .par_iter()
                    .map(|series| series.take_indices(indices))
            })
            .collect();
        Self::new_no_checks(series)
    }

    pub fn remove_columns(&self, names: &[&str]) -> Self {
        let mut df: Option<DataFrame> = None;
        for name in names.iter() {
            df = match df {
                Some(df) => Some(df.drop(name)),
                None => Some(self.drop(name)),
            }
        }
        df.unwrap()
    }
}

pub struct IdxHash {
    pub idx: usize,
    pub hash: u64,
}

pub type Idx = usize;

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

fn compare_df_row2(df1: &DataFrame, idx1: usize, df2: &DataFrame, idx2: usize) -> bool {
    for (series1, series2) in df1.columns.iter().zip(df2.columns.iter()) {
        let v1 = series1.get(idx1);
        let v2 = series2.get(idx2);
        let is_equal = v1 == v2;
        if !is_equal {
            return false;
        }
    }
    true
}

pub fn compute_inner_join_indices(df1: &DataFrame, df2: &DataFrame) -> (Vec<Idx>, Vec<Idx>) {
    let n_threads = POOL.current_num_threads();
    let df1_split = split_df(df1, n_threads);
    let df2_split = split_df(df2, n_threads);

    let df1_hashes = hash_dataframes(&df1_split);
    let df2_hashes = hash_dataframes(&df2_split);

    let hash_tables = build_probe_table(&df1_hashes, df1);
    let offsets = compute_offsets(&df1_hashes);
    POOL.install(|| {
        df2_hashes
            .into_par_iter()
            .zip(offsets)
            .flat_map(|(hashes, offset)| {
                // TODO: Capacity?
                let mut output = Vec::<(Idx, Idx)>::new();
                for (idx, hash) in hashes.iter().enumerate() {
                    let row_idx = offset + idx;
                    let hash_table_idx = *hash % n_threads as u64;
                    let hashtable = hash_tables.get(hash_table_idx as usize).unwrap();
                    let entry = hashtable.raw_entry().from_hash(*hash, |idx_hash| {
                        idx_hash.hash == *hash && {
                            let entry_idx = idx_hash.idx;
                            compare_df_row2(df1, entry_idx, df2, row_idx)
                        }
                    });
                    if let Some((_, df1_indices)) = entry {
                        let tuples = df1_indices.iter().map(|df1_idx| (*df1_idx, row_idx));
                        output.extend(tuples);
                    }
                }
                output
            })
    })
    .unzip()
}

fn compute_offsets<T>(lists: &Vec<Vec<T>>) -> Vec<usize> {
    let mut offset = 0;
    let mut output = Vec::with_capacity(lists.len());
    for list in lists {
        output.push(offset);
        offset += list.len();
    }
    output
}

fn combine_dataframes(df1: &DataFrame, df2: &DataFrame) -> DataFrame {
    // TODO: Rename if column names collapse
    let mut columns = Vec::with_capacity(df1.columns_count() + df2.columns_count());
    for series in &df1.columns {
        columns.push(series.clone());
    }
    for series in &df2.columns {
        columns.push(series.clone());
    }
    DataFrame { columns }
}
