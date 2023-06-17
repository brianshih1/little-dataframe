use arrow2::array::{Array, BooleanArray};

use crate::{
    chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked},
    series::series_trait::SeriesTrait,
    types::DataType,
};

use super::Series;

impl Series {
    pub fn bool(&self) -> &BooleanChunked {
        if self.dtype() != DataType::Boolean {
            panic!("Expected Series to contain boolean type")
        }
        unsafe { &*(self.0.as_ref() as *const dyn SeriesTrait as *const BooleanChunked) }
    }

    pub fn i32(&self) -> &I32Chunked {
        if self.dtype() != DataType::Int32 {
            panic!("Expected Series to contain I32 type")
        }
        unsafe { &*(self.0.as_ref() as *const dyn SeriesTrait as *const I32Chunked) }
    }

    pub fn utf8(&self) -> &Utf8Chunked {
        if self.dtype() != DataType::Utf8 {
            panic!("Expected Series to contain Utf8 type")
        }
        unsafe { &*(self.0.as_ref() as *const dyn SeriesTrait as *const Utf8Chunked) }
    }
}
