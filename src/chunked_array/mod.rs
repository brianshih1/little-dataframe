use arrow2::{array, offset};
use smartstring::alias::String as SmartString;
use std::iter::Map;
use std::marker::PhantomData;
use std::slice::Iter;
use std::usize::MIN;

use arrow2::array::{Array, BooleanArray, Int32Array, PrimitiveArray, Utf8Array};
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType as ArrowDataType;

use crate::little_arrow::types::ArrayRef;
use crate::types::{DataType, LittleDataType};

use self::types::{AnyValue, I32Chunked, Utf8Chunked};

pub mod aggregate;
mod aggregate_test;
pub mod builder;
pub mod chunk_compare;
mod chunk_compare_test;
pub mod chunk_equal;
pub mod chunk_full;
pub mod chunk_get;
pub mod filter;
mod filter_test;
pub mod format;
mod iter;
mod iter_test;
mod mod_test;
pub mod sort;
mod sort_test;
pub mod test_utils;
pub mod to_vec;
pub mod types;
pub mod utils;
mod utils_test;

pub struct ChunkedArray<T: LittleDataType> {
    pub name: SmartString,
    pub chunks: Vec<ArrayRef>,
    // TODO: Finalize if this is number of chunks or total number of elements
    pub length: usize,
    phantom: PhantomData<T>,
}

unsafe impl<T> Send for ChunkedArray<T> where T: LittleDataType {}

pub type ChunkLenIter<'a> = std::iter::Map<std::slice::Iter<'a, ArrayRef>, fn(&ArrayRef) -> usize>;

impl<T> ChunkedArray<T>
where
    T: LittleDataType,
{
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn chunk_length_it<'a>(&self) -> ChunkLenIter {
        self.chunks.iter().map(|chunk| chunk.len())
    }

    pub fn from_chunks(name: &str, chunks: Vec<ArrayRef>) -> Self {
        let mut arr = ChunkedArray {
            name: name.into(),
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

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let (offset, length) = compute_offset_and_length(offset, length, self.length);
        if offset >= self.length {
            panic!("Offset exceeded array length")
        }
        let mut output_chunks = Vec::new();
        let mut remaining_offset = offset;
        let mut remaining_length = length;
        for chunk in &self.chunks {
            if remaining_offset > chunk.len() {
                remaining_offset -= chunk.len();
                continue;
            }
            let chunk_len = chunk.len();
            let slice_len = chunk_len - remaining_offset;
            if remaining_length > slice_len {
                output_chunks.push(chunk.sliced(remaining_offset, slice_len));
                remaining_offset = 0;
                remaining_length = remaining_length - slice_len;
                continue;
            } else {
                output_chunks.push(
                    chunk.sliced(remaining_offset, std::cmp::min(slice_len, remaining_length)),
                );
                break;
            }
        }
        Self::from_chunks(&self.name, output_chunks)
    }
}

// returns (new_offset, new_length)
pub fn compute_offset_and_length(offset: usize, length: usize, array_len: usize) -> (usize, usize) {
    // if length > array_len {
    //     (0, std::cmp::min(array_len, array_len - offset))
    // } else {
    //     // we want to make sure if the offset + length exceeds array,
    //     // the new length is just the remaining array
    //     (offset, std::cmp::min(length, array_len - offset))
    // }
    (offset, std::cmp::min(length, array_len - offset))
}
