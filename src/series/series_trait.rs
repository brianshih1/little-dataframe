use std::collections::hash_map::RandomState;

use crate::{
    chunked_array::types::{AnyValue, BooleanChunked},
    core::field::Field,
    dataframe::groupby::GroupsProxy,
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

    unsafe fn equal_element(
        &self,
        idx_self: usize,
        other_series: &Series,
        idx_other: usize,
    ) -> bool;

    fn take_indices(&self, indices: &[usize]) -> Series;

    fn filter(&self, _filter: &BooleanChunked) -> Series;

    fn field(&self) -> Field;

    fn agg_min(&self, groups: &GroupsProxy) -> Series;
}
