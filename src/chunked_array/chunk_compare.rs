use arrow2::compute::comparison;

use crate::little_arrow::types::ArrayRef;

use super::{
    types::{BooleanChunked, I32Chunked, Utf8Chunked},
    utils::align_chunked_arrays,
    ChunkedArray,
};

pub trait ChunkCompare<Rhs> {
    fn equal(&self, rhs: Rhs) -> BooleanChunked;
}

impl ChunkCompare<&I32Chunked> for I32Chunked {
    fn equal(&self, rhs: &I32Chunked) -> BooleanChunked {
        let aligned_rhs = align_chunked_arrays(rhs, self);
        let chunks = self
            .iter_primitive()
            .zip(aligned_rhs.iter_primitive())
            .map(|(a, b)| {
                let is_eq = comparison::eq_and_validity(a, b);
                Box::new(is_eq) as ArrayRef
            })
            .collect::<Vec<_>>();
        ChunkedArray::from_chunks(self.name(), chunks)
    }
}

impl ChunkCompare<&BooleanChunked> for BooleanChunked {
    fn equal(&self, rhs: &BooleanChunked) -> BooleanChunked {
        let aligned_rhs = align_chunked_arrays(rhs, self);
        let chunks = self
            .iter_primitive()
            .zip(aligned_rhs.iter_primitive())
            .map(|(a, b)| {
                let is_eq = comparison::eq_and_validity(a, b);
                Box::new(is_eq) as ArrayRef
            })
            .collect::<Vec<_>>();
        ChunkedArray::from_chunks(self.name(), chunks)
    }
}

impl ChunkCompare<&Utf8Chunked> for Utf8Chunked {
    fn equal(&self, rhs: &Utf8Chunked) -> BooleanChunked {
        let aligned_rhs = align_chunked_arrays(rhs, self);
        let chunks = self
            .iter_primitive()
            .zip(aligned_rhs.iter_primitive())
            .map(|(a, b)| {
                let is_eq = comparison::eq_and_validity(a, b);
                Box::new(is_eq) as ArrayRef
            })
            .collect::<Vec<_>>();
        ChunkedArray::from_chunks(self.name(), chunks)
    }
}
