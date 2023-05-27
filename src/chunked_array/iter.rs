use arrow2::array::BooleanArray;
use arrow2::array::*;

use super::types::{BooleanChunked, I32Chunked, Utf8Chunked};

impl BooleanChunked {
    pub fn iter_primitive(&self) -> impl Iterator<Item = &BooleanArray> {
        self.chunks.iter().map(|chunk| {
            let arr = &**chunk;
            unsafe { &*(arr as *const dyn Array as *const BooleanArray) }
        })
    }
}

impl I32Chunked {
    pub fn iter_primitive(&self) -> impl Iterator<Item = &Int32Array> {
        self.chunks.iter().map(|chunk| {
            let arr = &**chunk;
            unsafe { &*(arr as *const dyn Array as *const Int32Array) }
        })
    }
}

impl Utf8Chunked {
    pub fn iter_primitive(&self) -> impl Iterator<Item = &Utf8Array<i64>> {
        self.chunks.iter().map(|chunk| {
            let arr = &**chunk;
            unsafe { &*(arr as *const dyn Array as *const Utf8Array<i64>) }
        })
    }
}

impl<'a> IntoIterator for &'a BooleanChunked {
    type Item = Option<bool>;

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_primitive().flatten().into_iter())
    }
}

impl<'a> IntoIterator for &'a I32Chunked {
    type Item = Option<i32>;

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.iter_primitive()
                .flatten()
                .map(|e| e.copied())
                .into_iter(),
        )
    }
}

impl<'a> IntoIterator for &'a Utf8Chunked {
    type Item = Option<&'a str>;

    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter_primitive().flatten().into_iter())
    }
}
