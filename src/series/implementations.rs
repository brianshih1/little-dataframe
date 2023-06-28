use std::{cmp::min, collections::hash_map::RandomState, sync::Arc};

use crate::{
    chunked_array::{
        builder::NewFrom,
        chunk_equal::ChunkEqualElement,
        chunk_get::ChunkGet,
        filter::ChunkedArrayFilter,
        types::{AnyValue, BooleanChunked, I32Chunked, Utf8Chunked},
    },
    core::field::Field,
    dataframe::groupby::GroupsProxy,
    hashing::VecHash,
    types::DataType,
};

use super::{constructor::IntoSeries, series_trait::SeriesTrait, Series, SeriesWrap};

impl SeriesTrait for SeriesWrap<BooleanChunked> {
    fn dtype(&self) -> DataType {
        DataType::Boolean
    }

    fn len(&self) -> usize {
        self.0.length
    }

    fn name(&self) -> &str {
        &self.0.name
    }

    fn vec_hash(&self, hasher: RandomState, buf: &mut Vec<u64>) {
        self.0.vec_hash(hasher, buf)
    }

    fn vec_hash_combine(&self, hasher: RandomState, buf: &mut Vec<u64>) {
        self.0.vec_hash_combine(hasher, buf)
    }

    fn rechunk(&self) -> super::Series {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> Series {
        let chunked = self.0.slice(offset, length);
        chunked.into_series()
    }

    fn get(&self, idx: usize) -> Option<AnyValue> {
        self.0.get_value(idx)
    }

    unsafe fn equal_element(
        &self,
        idx_self: usize,
        other_series: &Series,
        idx_other: usize,
    ) -> bool {
        self.0.equal_element(idx_self, other_series, idx_other)
    }

    fn take_indices(&self, indices: &[usize]) -> Series {
        // TODO: Optimizations since performing get is expensive
        let value = indices
            .iter()
            .map(|idx| {
                let v = self.get(*idx);
                v.map(|v| match v {
                    AnyValue::Boolean(v) => v,
                    AnyValue::Utf8(_) => unreachable!(),
                    AnyValue::Int32(_) => unreachable!(),
                })
            })
            .collect::<Vec<Option<bool>>>();
        Series::from_slice_options(self.name(), &value)
    }

    fn filter(&self, filter: &BooleanChunked) -> Series {
        self.0.filter(filter).into_series()
    }

    fn field(&self) -> Field {
        Field {
            name: self.name().into(),
            dtype: self.dtype(),
        }
    }

    fn agg_min(&self, groups: &GroupsProxy) -> Series {
        todo!()
    }
}

impl SeriesTrait for SeriesWrap<I32Chunked> {
    fn dtype(&self) -> DataType {
        DataType::Int32
    }

    fn len(&self) -> usize {
        self.0.length
    }

    fn name(&self) -> &str {
        &self.0.name
    }

    fn vec_hash(&self, hasher: RandomState, buf: &mut Vec<u64>) {
        self.0.vec_hash(hasher, buf)
    }

    fn vec_hash_combine(&self, hasher: RandomState, buf: &mut Vec<u64>) {
        self.0.vec_hash_combine(hasher, buf)
    }

    fn rechunk(&self) -> super::Series {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> super::Series {
        let chunked = self.0.slice(offset, length);
        chunked.into_series()
    }

    fn get(&self, idx: usize) -> Option<AnyValue> {
        self.0.get_value(idx)
    }

    unsafe fn equal_element(
        &self,
        idx_self: usize,
        other_series: &Series,
        idx_other: usize,
    ) -> bool {
        self.0.equal_element(idx_self, other_series, idx_other)
    }

    fn take_indices(&self, indices: &[usize]) -> Series {
        // TODO: Optimizations since performing get is expensive
        let value = indices
            .iter()
            .map(|idx| {
                let v = self.get(*idx);
                v.map(|v| match v {
                    AnyValue::Boolean(_) => unreachable!(),
                    AnyValue::Utf8(_) => unreachable!(),
                    AnyValue::Int32(v) => v,
                })
            })
            .collect::<Vec<Option<i32>>>();
        Series::from_slice_options(self.name(), &value)
    }

    fn filter(&self, filter: &BooleanChunked) -> Series {
        self.0.filter(filter).into_series()
    }

    fn field(&self) -> Field {
        Field {
            name: self.name().into(),
            dtype: self.dtype(),
        }
    }

    fn agg_min(&self, groups: &GroupsProxy) -> Series {
        let arr = self.0.iter_primitive().next().unwrap();

        let values: Vec<i32> = groups
            .all
            .iter()
            .enumerate()
            .map(|(_, indices)| {
                // TODO: Parallelize
                let min = indices.iter().fold(i32::MAX, |acc, &idx| {
                    let v = arr.get(idx as usize).unwrap();
                    min(acc, v)
                });
                min
            })
            .collect();
        Series::new(self.name(), &values)
    }
}

impl SeriesTrait for SeriesWrap<Utf8Chunked> {
    fn dtype(&self) -> DataType {
        DataType::Utf8
    }

    fn len(&self) -> usize {
        self.0.length
    }

    fn name(&self) -> &str {
        &self.0.name
    }

    fn vec_hash(&self, hasher: RandomState, buf: &mut Vec<u64>) {
        self.0.vec_hash(hasher, buf)
    }

    fn vec_hash_combine(&self, hasher: RandomState, buf: &mut Vec<u64>) {
        self.0.vec_hash_combine(hasher, buf)
    }

    fn rechunk(&self) -> super::Series {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> super::Series {
        let chunked = self.0.slice(offset, length);
        chunked.into_series()
    }

    fn get(&self, idx: usize) -> Option<AnyValue> {
        self.0.get_value(idx)
    }

    unsafe fn equal_element(
        &self,
        idx_self: usize,
        other_series: &Series,
        idx_other: usize,
    ) -> bool {
        self.0.equal_element(idx_self, other_series, idx_other)
    }

    fn take_indices(&self, indices: &[usize]) -> Series {
        // TODO: Optimizations since performing get is expensive
        let value = indices
            .iter()
            .map(|idx| {
                let v = self.get(*idx);
                v.map(|v| match v {
                    AnyValue::Boolean(_) => unreachable!(),
                    AnyValue::Utf8(v) => v,
                    AnyValue::Int32(v) => unreachable!(),
                })
            })
            .collect::<Vec<Option<&str>>>();
        Series::from_slice_options(self.name(), &value)
    }

    fn filter(&self, filter: &BooleanChunked) -> Series {
        self.0.filter(filter).into_series()
    }

    fn field(&self) -> Field {
        Field {
            name: self.name().into(),
            dtype: self.dtype(),
        }
    }

    fn agg_min(&self, groups: &GroupsProxy) -> Series {
        todo!()
    }
}
