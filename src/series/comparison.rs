use crate::{
    chunked_array::{chunk_compare::ChunkCompare, types::BooleanChunked},
    types::DataType,
};

use super::Series;

impl ChunkCompare<&Series> for Series {
    fn equal(&self, rhs: &Series) -> BooleanChunked {
        assert_eq!(self.dtype(), rhs.dtype());
        match self.dtype() {
            DataType::Int32 => self.i32().equal(rhs.i32()),
            DataType::Utf8 => self.utf8().equal(rhs.utf8()),
            DataType::Boolean => self.bool().equal(rhs.bool()),
        }
    }
}
