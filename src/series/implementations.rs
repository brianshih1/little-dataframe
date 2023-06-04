use std::{collections::hash_map::RandomState, sync::Arc};

use crate::{
    chunked_array::{
        chunk_equal::ChunkEqualElement,
        chunk_get::ChunkGet,
        types::{AnyValue, BooleanChunked, I32Chunked, Utf8Chunked},
    },
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

    fn vec_hash(&self, _hasher: RandomState, buf: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(&self, _hasher: RandomState, buf: &mut Vec<u64>) {
        todo!()
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

    fn vec_hash(&self, _hasher: RandomState, buf: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(&self, _hasher: RandomState, buf: &mut Vec<u64>) {
        todo!()
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

    fn vec_hash(&self, _hasher: RandomState, buf: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(
        &self,
        _hasher: std::collections::hash_map::RandomState,
        buf: &mut Vec<u64>,
    ) {
        todo!()
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
}
