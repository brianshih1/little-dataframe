use super::{builder::NewFrom, ChunkedArray};

#[test]
fn bool_non_null_iter() {
    let arr = ChunkedArray::new("", &vec![true, false]);
    let it = arr.into_iter();
    let res = it.collect::<Vec<Option<bool>>>();
    assert_eq!(res, vec![Some(true), Some(false)])
}

#[test]
fn bool_null_iter() {
    let arr = ChunkedArray::from_slice_options("", &vec![None, Some(true), None, Some(false)]);
    let it = arr.into_iter();
    let res = it.collect::<Vec<Option<bool>>>();
    assert_eq!(res, vec![None, Some(true), None, Some(false)])
}
