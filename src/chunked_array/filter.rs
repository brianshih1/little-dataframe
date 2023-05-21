use arrow2::array::Int32Array;
use arrow2::compute::filter::filter as arrow_filter;

use crate::types::LittleDataType;

use super::{
    types::{BooleanChunked, BooleanType},
    ChunkedArray,
};

pub trait ChunkedArrayFilter<T: LittleDataType> {
    fn filter(&self, mask: &BooleanChunked) -> ChunkedArray<T>;
}

impl ChunkedArrayFilter<BooleanType> for Int32Array {
    fn filter(&self, mask: &BooleanChunked) -> BooleanChunked {
        todo!()
    }
}
