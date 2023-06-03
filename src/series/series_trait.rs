use std::collections::hash_map::RandomState;

use crate::{
    chunked_array::types::AnyValue,
    types::{DataType, LittleDataType},
};

use super::Series;

pub trait SeriesTrait: Send + Sync {
    fn dtype(&self) -> DataType;

    fn len(&self) -> usize;

    fn name(&self) -> &str;

    fn vec_hash(&self, _hasher: RandomState, buf: &mut Vec<u64>);

    fn vec_hash_combine(&self, _hasher: RandomState, buf: &mut Vec<u64>);

    // Converts the Series to a single chunk
    fn rechunk(&self) -> Series;

    fn slice(&self, offset: usize, length: usize) -> Series;

    fn get(&self, idx: usize) -> Option<AnyValue>;
}
