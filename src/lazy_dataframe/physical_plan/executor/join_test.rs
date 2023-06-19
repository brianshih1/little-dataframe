use crate::{
    chunked_array::builder::NewFrom,
    dataframe::{join::JoinType, DataFrame},
    lazy_dataframe::expr::col,
    series::Series,
};

#[test]
fn test_join() {
    let df1 = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "bar", "baz"]),
        Series::from_vec("points", &vec![0, 10, 20]),
    ]);

    let df2 = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "baz"]),
        Series::from_vec("blocks", &vec![0, 2]),
    ]);
    let res = df1
        .lazy()
        .join(
            vec![col("name")],
            df2.lazy(),
            vec![col("name")],
            JoinType::Inner,
        )
        .collect();

    let expected_df = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "baz"]),
        Series::from_vec("points", &vec![0, 20]),
        Series::from_vec("blocks", &vec![0, 2]),
    ]);
    assert_eq!(&res, &expected_df);
}
