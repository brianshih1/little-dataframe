use std::collections::hash_map::RandomState;

use hashbrown::{hash_map::RawEntryMut, HashMap};
use rayon::prelude::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    core::{sync_ptr::SyncPtr, POOL},
    dataframe::{
        join::{compare_df_row, IdxHash},
        utils::split_df,
    },
    hashing::{
        hash_dataframes,
        partition::{_set_partition_size, this_partition},
    },
    series::Series,
};

mod mod_test;
use super::DataFrame;

#[derive(Debug)]
pub enum GroupsProxy {
    Idx(GroupsIdx),
    Slice,
}

#[derive(Debug)]
pub struct GroupsIdx {
    first: Vec<u32>,
    all: Vec<Vec<u32>>,
}

impl DataFrame {
    pub fn compute_group_proxy(&self, by: Vec<Series>) -> GroupsProxy {
        assert_ne!(by.len(), 0);
        assert_eq!(self.rows_count(), by[0].len());
        let key_df = DataFrame::new(by);
        let n_threads = _set_partition_size();
        let hasher = RandomState::default();
        let df_split = split_df(&key_df, n_threads);
        let hashes = hash_dataframes(&df_split, &hasher);
        println!("Hashed: {:?}", hashes);
        let tuples: Vec<(Vec<u32>, Vec<Vec<u32>>)> = POOL
            .install(|| {
                (0..n_threads).into_par_iter().map(|thread_no| {
                    let mut first_indices = Vec::<u32>::new();
                    let mut grouped_indices = Vec::<Vec<u32>>::new();
                    // map from idx to the index inside first_vec.
                    let mut hashmap = HashMap::<IdxHash, usize>::new(); // TODO: Capacity
                    let mut offset: usize = 0;
                    for hashes in &hashes {
                        hashes.iter().enumerate().for_each(|(idx, hash)| {
                            let hash = *hash;
                            if this_partition(hash, thread_no as u64, n_threads as u64) {
                                let idx = offset + idx;
                                let entry = hashmap.raw_entry_mut().from_hash(hash, |idx_hash| {
                                    idx_hash.hash == hash && {
                                        let entry_idx = idx_hash.idx;
                                        compare_df_row(&key_df, idx as usize, entry_idx)
                                    }
                                });
                                match entry {
                                    RawEntryMut::Occupied(mut occupied) => {
                                        let (_, v) = occupied.get_key_value_mut();
                                        grouped_indices[*v].push(idx as u32);
                                    }
                                    RawEntryMut::Vacant(entry) => {
                                        entry.insert_hashed_nocheck(
                                            hash,
                                            IdxHash {
                                                idx: idx as usize,
                                                hash,
                                            },
                                            first_indices.len(),
                                        );
                                        first_indices.push(idx as u32);
                                        grouped_indices.push(vec![idx as u32]);
                                    }
                                }
                            }
                        });
                        offset += hashes.len();
                    }
                    (first_indices, grouped_indices)
                })
            })
            .collect();
        let (first_indices, grouped_indices) = join_group_indices(tuples);
        GroupsProxy::Idx(GroupsIdx {
            first: first_indices,
            all: grouped_indices,
        })
    }
}

fn join_group_indices(arr: Vec<(Vec<u32>, Vec<Vec<u32>>)>) -> (Vec<u32>, Vec<Vec<u32>>) {
    let output_len = arr.iter().map(|v| v.0.len()).sum::<usize>();
    let mut first_vec: Vec<u32> = Vec::with_capacity(output_len);
    let mut all_vecs: Vec<Vec<u32>> = Vec::with_capacity(output_len);

    let offsets = arr
        .iter()
        .scan(0, |acc, v| {
            let pre = *acc;
            let len = v.0.len();
            *acc += len;
            Some(pre)
        })
        .collect::<Vec<usize>>();

    let first_vec_ptr = SyncPtr::new(first_vec.as_mut_ptr());
    let all_vec_ptr = SyncPtr::new(all_vecs.as_mut_ptr());

    POOL.install(|| {
        arr.into_par_iter().zip(offsets).for_each(
            |((first_indices, mut all_indices), offset)| unsafe {
                let first_ptr = first_vec_ptr.get().add(offset);
                let all_ptr = all_vec_ptr.get().add(offset);

                std::ptr::copy_nonoverlapping(
                    first_indices.as_ptr(),
                    first_ptr,
                    first_indices.len(),
                );

                std::ptr::copy_nonoverlapping(all_indices.as_ptr(), all_ptr, all_indices.len());
                all_indices.set_len(0);
            },
        )
    });
    unsafe {
        first_vec.set_len(output_len);
        all_vecs.set_len(output_len);
    }
    (first_vec, all_vecs)
}
