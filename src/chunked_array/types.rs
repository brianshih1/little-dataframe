use crate::types::{DataType, LittleDataType};

use super::ChunkedArray;

pub struct BooleanType {}

impl LittleDataType for BooleanType {
    fn get_dtype() -> DataType {
        DataType::Boolean
    }
}

pub type BooleanChunked = ChunkedArray<BooleanType>;

pub struct I32Type {}

impl LittleDataType for I32Type {
    fn get_dtype() -> DataType {
        DataType::Int32
    }
}

pub type I32Chunked = ChunkedArray<I32Type>;

pub struct Utf8Type {}

impl LittleDataType for Utf8Type {
    fn get_dtype() -> DataType {
        DataType::Utf8
    }
}

pub type Utf8Chunked = ChunkedArray<Utf8Type>;

#[derive(Debug, Clone, PartialEq)]
pub enum AnyValue<'a> {
    Boolean(bool),
    Utf8(&'a str),
    Int32(i32),
}
