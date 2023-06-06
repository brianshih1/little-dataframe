use std::{fmt::Debug, ops::Deref, sync::Arc};

use crate::{
    chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked},
    types::DataType,
};

use self::series_trait::SeriesTrait;

pub mod constructor;
pub mod constructor_test;
pub mod implementations;
pub mod series_trait;

#[derive(Clone)]
pub struct Series(pub Arc<dyn SeriesTrait>);

pub struct SeriesWrap<T>(T);

impl Deref for Series {
    type Target = dyn SeriesTrait;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl Debug for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let chunked_arr = self.0.as_ref();
        match self.dtype() {
            DataType::Int32 => {
                let chunked_arr_ref = chunked_arr as *const dyn SeriesTrait as *const I32Chunked;
                let chunked_arr = unsafe { &*chunked_arr_ref };
                write!(f, "{:?}", chunked_arr).unwrap();
            }
            DataType::Utf8 => {
                let chunked_arr_ref = chunked_arr as *const dyn SeriesTrait as *const Utf8Chunked;
                let chunked_arr = unsafe { &*chunked_arr_ref };
                write!(f, "{:?}", chunked_arr).unwrap();
            }
            DataType::Boolean => {
                let chunked_arr_ref =
                    chunked_arr as *const dyn SeriesTrait as *const BooleanChunked;
                let chunked_arr = unsafe { &*chunked_arr_ref };
                write!(f, "{:?}", chunked_arr).unwrap();
            }
        }
        write!(f, "")
    }
}

// This is very inefficient and should only be used in tests
impl PartialEq for Series {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        if self.name() != other.name() {
            return false;
        }
        for i in 0..self.len() {
            if self.get(i) != other.get(i) {
                return false;
            }
        }
        true
    }
}
