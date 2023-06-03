use arrow2::array::{Array, BooleanArray, Int32Array, Utf8Array};

use crate::little_arrow::types::ArrayRef;

use super::{
    builder::NewFrom,
    types::{AnyValue, BooleanChunked, I32Chunked, Utf8Chunked},
    ChunkedArray,
};

pub trait ChunkGet {
    fn get_value(&self, index: usize) -> Option<AnyValue>;
}

impl ChunkGet for BooleanChunked {
    fn get_value(&self, index: usize) -> Option<AnyValue> {
        let (chunk_idx, idx) = get_chunk_idx(&self.chunks, index);
        let chunk = &self.chunks[chunk_idx];
        let arr = &**chunk;
        let chunk = unsafe { &*(arr as *const dyn Array as *const BooleanArray) };
        chunk.get(idx).map(|any_value| AnyValue::Boolean(any_value))
    }
}

impl ChunkGet for Utf8Chunked {
    fn get_value(&self, index: usize) -> Option<AnyValue> {
        let (chunk_idx, idx) = get_chunk_idx(&self.chunks, index);
        let chunk = &self.chunks[chunk_idx];
        let arr = &**chunk;
        let chunk = unsafe { &*(arr as *const dyn Array as *const Utf8Array<i64>) };
        chunk.get(idx).map(|any_value| AnyValue::Utf8(any_value))
    }
}

impl ChunkGet for I32Chunked {
    fn get_value(&self, index: usize) -> Option<AnyValue> {
        let (chunk_idx, idx) = get_chunk_idx(&self.chunks, index);
        let chunk = &self.chunks[chunk_idx];
        let arr = &**chunk;
        let chunk = unsafe { &*(arr as *const dyn Array as *const Int32Array) };
        chunk.get(idx).map(|any_value| AnyValue::Int32(any_value))
    }
}

// Returns (chunk_idx, idx)
fn get_chunk_idx(chunks: &Vec<ArrayRef>, idx: usize) -> (usize, usize) {
    let mut remaining_idx = idx;
    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        if remaining_idx + 1 > chunk.len() {
            remaining_idx -= chunk.len();
            continue;
        }
        return (chunk_idx, remaining_idx);
    }
    panic!("idx exceeds chunk length")
}

#[test]
fn test_get_chunk_idx() {
    let arr = ChunkedArray::from_lists("", vec![&vec![0, 1, 2], &vec![3, 4, 5]]);
    let (chunk_idx, idx) = get_chunk_idx(&arr.chunks, 4);
    assert_eq!((chunk_idx, idx), (1, 1));

    let (chunk_idx, idx) = get_chunk_idx(&arr.chunks, 5);
    assert_eq!((chunk_idx, idx), (1, 2));
}

#[test]
fn test_boolean_get() {
    let arr = ChunkedArray::from_lists("", vec![&vec![true, true, true], &vec![false]]);
    let value = arr.get_value(3);
    assert_eq!(value, Some(AnyValue::Boolean(false)));
}

#[test]
fn test_i32_get() {
    let arr = ChunkedArray::from_lists("", vec![&vec![0, 1, 2], &vec![3, 4, 5]]);
    let value = arr.get_value(2);
    assert_eq!(value, Some(AnyValue::Int32(2)));
}
