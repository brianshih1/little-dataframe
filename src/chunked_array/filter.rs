use arrow2::array::Int32Array;
use arrow2::compute::filter::filter as arrow_filter;

use crate::types::LittleDataType;

use super::{
    types::{BooleanChunked, BooleanType, I32Chunked, I32Type},
    utils::align_chunked_arrays,
    ChunkedArray,
};

pub trait ChunkedArrayFilter<T: LittleDataType> {
    fn filter(&self, mask: &BooleanChunked) -> ChunkedArray<T>;
}

impl ChunkedArrayFilter<I32Type> for I32Chunked {
    fn filter(&self, mask: &BooleanChunked) -> Self {
        let aligned_mask = align_chunked_arrays(mask, self);
        let chunks = self
            .iter_primitive()
            .zip(aligned_mask.iter_primitive())
            .map(|(primitive_arr, mask)| {
                let filtered = arrow_filter(primitive_arr, mask).unwrap();
                filtered
            })
            .collect();
        I32Chunked::from_chunks(&self.name, chunks)
    }
}
