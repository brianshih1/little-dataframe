use arrow2::array::{BooleanArray, Int32Array, PrimitiveArray, Utf8Array};
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType as ArrowDataType;

use crate::little_arrow::types::ArrayRef;

pub struct ChunkedArray {
    pub chunks: Vec<ArrayRef>,
    pub length: usize,
}

pub trait NewFrom<TItem> {
    fn new(name: &str, v: &[TItem]) -> Self;

    fn from_slice_options(name: &str, v: &[Option<TItem>]) -> Self;
}

impl NewFrom<i32> for ChunkedArray {
    fn new(name: &str, v: &[i32]) -> Self {
        let primitive_array = Int32Array::from_iter(v.iter().copied().map(Some));
        ChunkedArray {
            chunks: vec![Box::new(primitive_array)],
            length: v.len(),
        }
    }

    fn from_slice_options(name: &str, v: &[Option<i32>]) -> Self {
        todo!()
    }
}

impl NewFrom<&str> for ChunkedArray {
    fn new(name: &str, v: &[&str]) -> Self {
        let primitive_array = Utf8Array::<i32>::from_iter(v.iter().map(|i| Some(i)));
        ChunkedArray {
            chunks: vec![Box::new(primitive_array)],
            length: v.len(),
        }
    }

    fn from_slice_options(name: &str, v: &[Option<&str>]) -> Self {
        todo!()
    }
}

mod Test {
    use super::{ChunkedArray, NewFrom};

    #[test]
    fn experiment() {
        let foo = ChunkedArray::new("", &[12]);
        let foo = vec!["hello"];
        let foo2 = ChunkedArray::new("", &vec!["hello"]);
    }
}
