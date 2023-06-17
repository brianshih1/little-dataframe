use arrow2::bitmap::Bitmap;

use super::types::{BooleanChunked, I32Chunked};

impl I32Chunked {
    pub fn to_vec(&self) -> Vec<i32> {
        let mut list = Vec::with_capacity(self.length);
        self.iter_primitive().for_each(|primitive_arr| {
            let buffer = primitive_arr.values();
            // Buffer derefs to a slice
            list.extend_from_slice(buffer);
        });
        list
    }

    pub fn to_vec_options(&self) -> Vec<Option<i32>> {
        let it = self.into_iter();
        it.collect()
    }
}

impl BooleanChunked {
    pub fn to_vec(&self) -> Vec<bool> {
        let mut list = Vec::with_capacity(self.length);
        self.iter_primitive().for_each(|primitive_arr| {
            let buffer = primitive_arr.values();
            // Buffer derefs to a slice
            list.extend_from_slice(to_bool_vec(buffer).as_slice());
        });
        list
    }

    pub fn to_vec_options(&self) -> Vec<Option<bool>> {
        let it = self.into_iter();
        it.collect()
    }
}

pub fn to_bool_vec(map: &Bitmap) -> Vec<bool> {
    let (bytes, offset, len) = map.as_slice();
    let mut bools = Vec::with_capacity(len);
    for i in 0..len {
        let byte_idx = (i + offset) / 8;
        let bit_idx = (i + offset) % 8;
        let mask = 1 << bit_idx;
        bools.push((bytes[byte_idx] & mask) != 0);
    }
    bools
}
