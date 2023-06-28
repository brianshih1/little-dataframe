use crate::{
    chunked_array::builder::NewFrom,
    dataframe::DataFrame,
    lazy_dataframe::expr::{col, AggExpr, Expr},
    series::Series,
};

#[test]
fn test_simple_groupby_agg() {
    let df = DataFrame::new(vec![
        Series::from_vec("name", &vec!["a", "b", "a", "b", "c", "c"]),
        Series::from_vec("points", &vec![1, 2, 3, 2, 1, 0]),
    ]);

    let computed_df = df
        .lazy()
        .groupby(vec![col("name")])
        .agg(vec![col("points").min()])
        .collect();
    println!("Groupby: {computed_df:?}");

    // let expected_df = DataFrame::new(vec![
    //     Series::from_vec("name", &vec!["a", "c", "b"]),
    //     Series::from_vec("points", &vec![1, 0, 2]),
    // ])
    // assert_eq!(&computed_df, &expected_df);
}
