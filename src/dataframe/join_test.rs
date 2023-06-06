use crate::{
    chunked_array::{builder::NewFrom, types::AnyValue},
    series::Series,
};

use super::DataFrame;

#[test]
fn test_create_df_from_slice() {
    let df = DataFrame::new(vec![
        Series::from_slice_options("age", &vec![Some(0), None, Some(1), Some(2)]),
        Series::from_slice_options("str", &vec![Some("0"), None, Some("1"), Some("2")]),
    ]);
    let sliced = df.create_df_from_slice(&[1, 3]);

    let expected_df = DataFrame::new(vec![
        Series::from_slice_options("age", &vec![None, Some(2)]),
        Series::from_slice_options("str", &vec![None, Some("2")]),
    ]);

    assert_eq!(sliced, expected_df);
}

#[test]
fn test_inner_join() {
    let df1 = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "bar", "baz"]),
        Series::from_vec("points", &vec![0, 10, 20]),
    ]);

    let df2 = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "baz"]),
        Series::from_vec("blocks", &vec![0, 2]),
    ]);

    let joined = df1.inner_join(vec!["name"], &df2, vec!["name"]);
    let expected_df = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "baz"]),
        Series::from_vec("points", &vec![0, 20]),
        Series::from_vec("blocks", &vec![0, 2]),
    ]);
    assert_eq!(&joined, &expected_df);
}
