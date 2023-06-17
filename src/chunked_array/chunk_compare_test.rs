use super::{builder::NewFrom, chunk_compare::ChunkCompare, test_utils::AssertUtils, ChunkedArray};

#[test]
fn test_compare_i32() {
    let c1 = ChunkedArray::from_lists("", vec![&vec![0, 1, 2], &vec![3, 4, 5]]);
    let c2 = ChunkedArray::from_lists("", vec![&vec![0, 1, 2, 3, 4, 5]]);
    let is_equal = c1.equal(&c2);
    assert_eq!(is_equal.to_vec(), vec![true, true, true, true, true, true]);

    let c1 = ChunkedArray::from_lists("", vec![&vec![0, 1, 2], &vec![3, 4, 5]]);
    let c2 = ChunkedArray::from_lists("", vec![&vec![0, 1, 2, 3, 4, 6]]);
    let is_equal = c1.equal(&c2);
    assert_eq!(is_equal.to_vec(), vec![true, true, true, true, true, false]);
}

#[test]
fn test_compare_utf8() {
    let c1 = ChunkedArray::from_lists("", vec![&vec!["foo", "bar"], &vec!["baz"]]);
    let c2 = ChunkedArray::from_lists("", vec![&vec!["foo", "bar", "lol"]]);
    let is_equal = c1.equal(&c2);
    assert_eq!(is_equal.to_vec(), vec![true, true, false]);
}
