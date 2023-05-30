use std::{
    collections::hash_map::RandomState,
    hash::{self, BuildHasher, Hash, Hasher},
};

use arrow2::array::Array;

use crate::chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked};

use super::VecHash;

impl VecHash for I32Chunked {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>) {
        self.iter_primitive().for_each(|chunked_array| {
            let arr = chunked_array.values().iter().copied().map(|ele| {
                let ele = ele as u64;
                folded_multiply(ele, MULTIPLE)
            });
            hashes.extend(arr);
        });
        let mut offset = 0;
        let null_hash = get_null_hash(&random_state);
        self.iter_primitive().for_each(|chunked_array| {
            if chunked_array.null_count() != 0 {
                chunked_array
                    .validity()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .for_each(|(idx, t)| {
                        if !t {
                            hashes[offset + idx] = null_hash;
                        }
                    })
            }
            offset += chunked_array.len();
        });
    }

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]) {
        let mut offset = 0;
        let null_hash = get_null_hash(&random_state);
        self.iter_primitive().for_each(|chunked_array| {
            if chunked_array.null_count() == 0 {
                chunked_array
                    .values()
                    .iter()
                    .copied()
                    .enumerate()
                    .for_each(|(idx, ele)| {
                        // TODO: Remove indexing operation
                        hashes[offset + idx] =
                            folded_multiply((ele as u64) ^ hashes[offset + idx], MULTIPLE);
                    })
            } else {
                chunked_array
                    .values()
                    .iter()
                    .copied()
                    .enumerate()
                    .zip(chunked_array.validity().unwrap().iter())
                    .for_each(|((idx, ele), is_valid)| {
                        if is_valid {
                            hashes[offset + idx] =
                                folded_multiply((ele as u64) ^ hashes[offset + idx], MULTIPLE);
                        } else {
                            hashes[offset + idx] =
                                folded_multiply(null_hash ^ hashes[offset + idx], MULTIPLE);
                        }
                    })
            }
            offset += chunked_array.len();
        })
    }
}

// *h = folded_multiply(v.as_u64() ^ *h, MULTIPLE);

impl VecHash for Utf8Chunked {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]) {
        todo!()
    }
}

impl VecHash for BooleanChunked {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]) {
        todo!()
    }
}

// Check out the AHash library:
// https://github.com/tkaitchuck/aHash/blob/f9acd508bd89e7c5b2877a9510098100f9018d64/src/operations.rs#L15
pub(crate) const fn folded_multiply(s: u64, by: u64) -> u64 {
    let result = (s as u128).wrapping_mul(by as u128);
    ((result & 0xffff_ffff_ffff_ffff) as u64) ^ ((result >> 64) as u64)
}

// See: https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/hashing/vector_hasher.rs#L44.
// This is an arbitrary prime number
const NULL_PRIME: usize = 3188347919usize;

fn get_null_hash(random_state: &RandomState) -> u64 {
    let mut hasher = random_state.build_hasher();
    NULL_PRIME.hash(&mut hasher);
    hasher.finish()
}

// See: https://github.com/tkaitchuck/aHash/blob/f9acd508bd89e7c5b2877a9510098100f9018d64/src/operations.rs#L4
const MULTIPLE: u64 = 6364136223846793005;
