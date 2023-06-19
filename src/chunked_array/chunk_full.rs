use super::{
    builder::NewFrom,
    types::{BooleanChunked, I32Chunked, Utf8Chunked},
    ChunkedArray,
};

pub trait ChunkFull<T> {
    fn full(name: &str, value: T, length: usize) -> Self;
}

impl ChunkFull<i32> for I32Chunked {
    fn full(name: &str, value: i32, length: usize) -> Self {
        let value = vec![value; length];
        ChunkedArray::new(name, &value)
    }
}

impl<'a> ChunkFull<&'a str> for Utf8Chunked {
    fn full(name: &str, value: &'a str, length: usize) -> Self {
        let value = vec![value; length];
        ChunkedArray::new(name, &value)
    }
}

impl ChunkFull<bool> for BooleanChunked {
    fn full(name: &str, value: bool, length: usize) -> Self {
        let value = vec![value; length];
        ChunkedArray::new(name, &value)
    }
}
