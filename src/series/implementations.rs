use crate::{
    chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked},
    types::DataType,
};

use super::{series_trait::SeriesTrait, SeriesWrap};

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

    fn vec_hash(&self, _hasher: std::collections::hash_map::RandomState, buf: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(
        &self,
        _hasher: std::collections::hash_map::RandomState,
        buf: &mut Vec<u64>,
    ) {
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

    fn vec_hash(&self, _hasher: std::collections::hash_map::RandomState, buf: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(
        &self,
        _hasher: std::collections::hash_map::RandomState,
        buf: &mut Vec<u64>,
    ) {
        todo!()
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

    fn vec_hash(&self, _hasher: std::collections::hash_map::RandomState, buf: &mut Vec<u64>) {
        todo!()
    }

    fn vec_hash_combine(
        &self,
        _hasher: std::collections::hash_map::RandomState,
        buf: &mut Vec<u64>,
    ) {
        todo!()
    }
}
