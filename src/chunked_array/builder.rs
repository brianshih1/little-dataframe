use std::marker::PhantomData;

use arrow2::array::{
    BooleanArray, Int32Array, MutableArray, MutableBooleanArray, MutablePrimitiveArray,
    MutableUtf8Array, Utf8Array,
};

use super::{
    types::{BooleanChunked, I32Chunked, Utf8Chunked},
    ChunkedArray,
};

pub trait NewFrom<TItem> {
    fn new(name: &str, v: &[TItem]) -> Self;

    fn from_slice_options(name: &str, v: &[Option<TItem>]) -> Self;

    fn from_vec(name: &str, v: &[TItem]) -> Self;

    #[cfg(test)]
    fn from_lists(name: &str, lists: Vec<&[TItem]>) -> Self;
}

impl NewFrom<bool> for BooleanChunked {
    fn new(name: &str, v: &[bool]) -> Self {
        let primitive_array = BooleanArray::from_iter(v.iter().copied().map(Some));
        let length = primitive_array.len();
        ChunkedArray {
            chunks: vec![Box::new(primitive_array)],
            length,
            phantom: PhantomData,
            name: name.into(),
        }
    }

    // TODO: Use MutableBooleanArray
    fn from_slice_options(name: &str, v: &[Option<bool>]) -> Self {
        let mut arr = MutableBooleanArray::new();
        v.iter().copied().for_each(|a| match a {
            Some(v) => arr.push(Some(v)),
            None => arr.push(None),
        });
        let primitive_arr = arr.as_box();
        let length = primitive_arr.len();
        ChunkedArray {
            chunks: vec![primitive_arr],
            length,
            phantom: PhantomData,
            name: name.into(),
        }
    }

    #[cfg(test)]
    fn from_lists(name: &str, lists: Vec<&[bool]>) -> Self {
        todo!()
    }

    fn from_vec(name: &str, v: &[bool]) -> Self {
        todo!()
    }
}

impl NewFrom<i32> for I32Chunked {
    fn new(name: &str, v: &[i32]) -> Self {
        let primitive_array = Int32Array::from_iter(v.iter().copied().map(Some));
        let length = primitive_array.len();
        ChunkedArray {
            chunks: vec![Box::new(primitive_array)],
            length: length,
            phantom: PhantomData,
            name: name.into(),
        }
    }

    fn from_slice_options(name: &str, v: &[Option<i32>]) -> Self {
        let mut arr = MutablePrimitiveArray::new();
        v.iter().copied().for_each(|a| match a {
            Some(v) => arr.push(Some(v)),
            None => arr.push(None),
        });
        let primitive_arr = arr.as_box();
        let length = primitive_arr.len();
        ChunkedArray {
            chunks: vec![primitive_arr],
            length,
            phantom: PhantomData,
            name: name.into(),
        }
    }

    #[cfg(test)]
    fn from_lists(name: &str, lists: Vec<&[i32]>) -> Self {
        use crate::little_arrow::types::ArrayRef;

        let primitive_arrays = lists
            .iter()
            .map(|list| Box::new(Int32Array::from_iter(list.iter().copied().map(Some))) as ArrayRef)
            .collect::<Vec<_>>();
        let mut arr = ChunkedArray {
            chunks: primitive_arrays,
            length: 0,
            phantom: PhantomData,
            name: name.into(),
        };
        arr.compute_len();
        arr
    }

    fn from_vec(name: &str, v: &[i32]) -> Self {
        todo!()
    }
}

impl NewFrom<&str> for Utf8Chunked {
    fn new(name: &str, v: &[&str]) -> Self {
        let primitive_array = Utf8Array::<i32>::from_iter(v.iter().map(|i| Some(i)));
        let length = primitive_array.len();
        ChunkedArray {
            chunks: vec![Box::new(primitive_array)],
            length,
            phantom: PhantomData,
            name: name.into(),
        }
    }

    fn from_slice_options(name: &str, v: &[Option<&str>]) -> Self {
        let mut arr = MutableUtf8Array::<i32>::new();
        v.iter().copied().for_each(|a| match a {
            Some(v) => arr.push(Some(v)),
            None => arr.push::<&str>(None),
        });
        let primitive_arr = arr.as_box();
        let length = primitive_arr.len();
        ChunkedArray {
            chunks: vec![primitive_arr],
            length,
            phantom: PhantomData,
            name: name.into(),
        }
    }

    #[cfg(test)]
    fn from_lists(name: &str, lists: Vec<&[&str]>) -> Self {
        todo!()
    }

    fn from_vec(name: &str, v: &[&str]) -> Self {
        todo!()
    }
}
