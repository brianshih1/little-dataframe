use arrow2::array::Utf8Array;

use crate::{
    chunked_array::{builder::NewFrom, chunk_get::ChunkGet, ChunkedArray},
    series::Series,
    types::DataType,
};

use super::types::{BooleanChunked, I32Chunked, Utf8Chunked};

pub trait ChunkEqualElement {
    unsafe fn equal_element(&self, idx_self: usize, _other: &Series, idx_other: usize) -> bool;
}

impl ChunkEqualElement for BooleanChunked {
    unsafe fn equal_element(&self, idx_self: usize, other: &Series, idx_other: usize) -> bool {
        assert!(other.dtype() == DataType::Boolean);
        let self_value = self.get_value(idx_self);
        let other_value = other.get(idx_other);
        self_value == other_value
    }
}

impl ChunkEqualElement for I32Chunked {
    unsafe fn equal_element(&self, idx_self: usize, other: &Series, idx_other: usize) -> bool {
        assert!(other.dtype() == DataType::Int32);
        let self_value = self.get_value(idx_self);
        let other_value = other.get(idx_other);
        self_value == other_value
    }
}

impl ChunkEqualElement for Utf8Chunked {
    unsafe fn equal_element(&self, idx_self: usize, other: &Series, idx_other: usize) -> bool {
        assert!(other.dtype() == DataType::Utf8);
        let self_value = self.get_value(idx_self);
        let other_value = other.get(idx_other);
        self_value == other_value
    }
}

#[test]
fn test_i32_eq() {
    let arr = ChunkedArray::from_lists("", vec![&vec![0, 1, 2], &vec![3, 4, 5]]);
    let series = Series::from_lists("", vec![&vec![0, 1, 2], &vec![2, 4, 5]]);
    let is_equal = unsafe { arr.equal_element(2, &series, 3) };
    assert_eq!(is_equal, true);

    let is_equal = unsafe { arr.equal_element(2, &series, 4) };
    assert_eq!(is_equal, false);
}
