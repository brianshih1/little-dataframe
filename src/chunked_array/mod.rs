use std::iter::Map;
use std::marker::PhantomData;
use std::slice::Iter;

use arrow2::array::{Array, BooleanArray, Int32Array, PrimitiveArray, Utf8Array};
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType as ArrowDataType;

use crate::little_arrow::types::ArrayRef;
use crate::types::{DataType, LittleDataType};

use self::types::{I32Chunked, Utf8Chunked};

pub mod aggregate;
mod aggregate_test;
pub mod builder;
pub mod filter;
mod filter_test;
pub mod format;
mod iter;
pub mod sort;
mod sort_test;
pub mod test_utils;
pub mod types;
pub mod utils;
mod utils_test;

pub struct ChunkedArray<T: LittleDataType> {
    pub chunks: Vec<ArrayRef>,
    // TODO: Finalize if this is number of chunks or total number of elements
    pub length: usize,
    phantom: PhantomData<T>,
}

pub type ChunkLenIter<'a> = std::iter::Map<std::slice::Iter<'a, ArrayRef>, fn(&ArrayRef) -> usize>;

impl<T> ChunkedArray<T>
where
    T: LittleDataType,
{
    pub fn chunk_length_it<'a>(&self) -> ChunkLenIter {
        self.chunks.iter().map(|chunk| chunk.len())
    }

    pub fn from_chunks(chunks: Vec<ArrayRef>) -> Self {
        let mut arr = ChunkedArray {
            chunks,
            length: 0,
            phantom: PhantomData,
        };
        arr.compute_len();
        arr
    }

    pub fn compute_len(&mut self) {
        let length = self.chunks.iter().fold(0, |acc, arr| acc + arr.len());
        self.length = length;
    }

    pub fn null_count(&self) -> usize {
        self.chunks
            .iter()
            .fold(0, |acc, arr| acc + arr.null_count())
    }
}
