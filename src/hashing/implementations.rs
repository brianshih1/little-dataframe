use std::{
    collections::hash_map::RandomState,
    hash::{self, BuildHasher, Hash, Hasher},
};

use arrow2::datatypes::DataType as ArrowDataType;
use arrow2::{array::Array, compute::cast::utf8_to_binary};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use super::VecHash;
use crate::chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked};

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

impl VecHash for Utf8Chunked {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>) {
        let null_hash = get_null_hash(&random_state);
        self.iter_primitive().for_each(|arr| {
            let offsets = arr.offsets();
            let casted = utf8_to_binary(arr, ArrowDataType::LargeBinary);
            if casted.null_count() == 0 {
                hashes.extend(
                    casted
                        .values_iter()
                        .map(|ele| xxh3_64_with_seed(ele, null_hash)),
                )
            } else {
                hashes.extend(casted.iter().map(|ele| match ele {
                    Some(ele) => xxh3_64_with_seed(ele, null_hash),
                    None => null_hash,
                }))
            }
        })
    }

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]) {
        let mut offset = 0;
        let null_hash = get_null_hash(&random_state);
        self.iter_primitive().for_each(|arr| {
            let casted = utf8_to_binary(arr, ArrowDataType::LargeBinary);
            if casted.null_count() == 0 {
                casted.values_iter().enumerate().for_each(|(idx, ele)| {
                    hashes[offset + idx] = _boost_hash_combine(
                        hashes[offset + idx],
                        xxh3_64_with_seed(ele, null_hash),
                    );
                })
            } else {
                casted
                    .values_iter()
                    .enumerate()
                    .zip(arr.validity().unwrap().iter())
                    .for_each(|((idx, ele), is_valid)| {
                        let hash = if is_valid {
                            xxh3_64_with_seed(ele, null_hash)
                        } else {
                            null_hash
                        };
                        hashes[offset + idx] = _boost_hash_combine(hashes[offset + idx], hash);
                    })
            }
            offset += casted.len();
        })
    }
}

impl VecHash for BooleanChunked {
    fn vec_hash(&self, random_state: RandomState, hashes: &mut Vec<u64>) {
        let true_hash = hash_boolean(&random_state, true);
        let false_hash = hash_boolean(&random_state, false);
        let null_hash = get_null_hash(&random_state);
        self.iter_primitive().for_each(|arr| {
            if arr.null_count() == 0 {
                hashes.extend(arr.values().iter().map(
                    |ele| {
                        if ele {
                            true_hash
                        } else {
                            false_hash
                        }
                    },
                ));
            } else {
                hashes.extend(arr.iter().map(|ele| match ele {
                    Some(value) => {
                        if value {
                            true_hash
                        } else {
                            false_hash
                        }
                    }
                    None => null_hash,
                }));
            }
        })
    }

    fn vec_hash_combine(&self, random_state: RandomState, hashes: &mut [u64]) {
        let mut offset = 0;
        let true_hash = hash_boolean(&random_state, true);
        let false_hash = hash_boolean(&random_state, false);
        let null_hash = get_null_hash(&random_state);
        self.iter_primitive().for_each(|arr| {
            if arr.null_count() == 0 {
                arr.values_iter().enumerate().for_each(|(idx, ele)| {
                    hashes[offset + idx] = _boost_hash_combine(
                        hashes[offset + idx],
                        if ele { true_hash } else { false_hash },
                    );
                })
            } else {
                arr.values_iter()
                    .enumerate()
                    .zip(arr.validity().unwrap().iter())
                    .for_each(|((idx, ele), is_valid)| {
                        let hash = if is_valid {
                            if ele {
                                true_hash
                            } else {
                                false_hash
                            }
                        } else {
                            null_hash
                        };
                        hashes[offset + idx] = _boost_hash_combine(hashes[offset + idx], hash);
                    })
            }
            offset += arr.len();
        })
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

// hash combine from c++' boost lib
#[inline]
pub fn _boost_hash_combine(l: u64, r: u64) -> u64 {
    l ^ r.wrapping_add(0x9e3779b9u64.wrapping_add(l << 6).wrapping_add(r >> 2))
}

fn hash_boolean(random_state: &RandomState, value: bool) -> u64 {
    let mut hasher = random_state.build_hasher();
    value.hash(&mut hasher);
    hasher.finish()
}
