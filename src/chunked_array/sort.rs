use std::{cmp::Ordering, iter::repeat, ptr::null};

use crate::core::POOL;
use arrow2::{
    array::PrimitiveArray,
    bitmap::{Bitmap, MutableBitmap},
};
use rayon::prelude::*;

use super::{builder::NewFrom, iter, types::I32Chunked};

pub trait ChunkedSort {
    fn sort(&self, descending: bool) -> Self;
}

pub fn sort_list<T, Fc, Fa>(
    list: &mut [T],
    descending: bool,
    compare_desc: Fc,
    compare_asc: Fa,
) -> ()
where
    Fc: Fn(&T, &T) -> Ordering + Send + Sync,
    Fa: Fn(&T, &T) -> Ordering + Send + Sync,
    T: Send,
{
    if descending {
        POOL.install(|| list.par_sort_unstable_by(compare_desc))
    } else {
        POOL.install(|| list.par_sort_unstable_by(compare_asc))
    }
}

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

impl ChunkedSort for I32Chunked {
    fn sort(&self, descending: bool) -> Self {
        if self.null_count() == 0 {
            let mut list = self.to_vec();
            sort_list(&mut list, descending, |a, b| b.cmp(a), |a, b| a.cmp(b));
            println!("List: {:?}", &list);
            I32Chunked::new("foo", &list)
        } else {
            let length = self.length;
            let mut list = Vec::with_capacity(self.length);
            let null_count = self.null_count();

            // Place all the nulls at the start
            list.extend(repeat(i32::default()).take(null_count));

            self.iter_primitive().for_each(|primitive_arr| {
                let iter = primitive_arr.iter().filter_map(|a| a.copied());
                list.extend(iter);
            });
            sort_list(
                &mut list[null_count..],
                descending,
                |a, b| b.cmp(a),
                |a, b| a.cmp(b),
            );
            let mut validity = MutableBitmap::with_capacity(length);
            validity.extend_constant(null_count, false);
            validity.extend_constant(length - null_count, true);
            let primitive_arr = PrimitiveArray::new(
                arrow2::datatypes::DataType::Int32,
                list.into(),
                validity.into(),
            );
            I32Chunked::from_chunks(vec![Box::new(primitive_arr)])
        }
    }
}
