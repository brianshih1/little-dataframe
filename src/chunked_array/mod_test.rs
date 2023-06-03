use crate::chunked_array::{builder::NewFrom, ChunkedArray};

#[test]
fn slice() {
    let arr = ChunkedArray::from_lists("", vec![&vec![1, 2, 3], &vec![4, 5, 6]]);
    let first_slice = arr.slice(2, 4);
    assert_eq!(first_slice.to_vec(), vec![3, 4, 5, 6]);

    let second_slice = arr.slice(1, 1);
    assert_eq!(second_slice.to_vec(), vec![2]);

    let slice = arr.slice(0, 6);
    assert_eq!(slice.to_vec(), vec![1, 2, 3, 4, 5, 6]);

    let slice = arr.slice(2, 20);
    assert_eq!(slice.to_vec(), vec![3, 4, 5, 6]);
}
