use crate::{
    chunked_array::builder::NewFrom,
    dataframe::{join::JoinType, DataFrame},
    lazy_dataframe::{
        expr::col,
        lit::{self, lit},
    },
    series::Series,
};

#[test]
fn test_inner_join_pushdown() {
    let df1 = DataFrame::new(vec![
        Series::from_vec("foo", &vec!["abc", "def", "ghi"]),
        Series::from_vec("idx1", &vec![0, 0, 1]),
    ]);

    let df2 = DataFrame::new(vec![
        Series::from_vec("bar", &vec![5, 6]),
        Series::from_vec("idx2", &vec![0, 1]),
    ]);

    let out = df1
        .lazy()
        .join(
            vec![col("idx1")],
            df2.lazy(),
            vec![col("idx2")],
            JoinType::Inner,
        )
        .filter(col("bar").eq(lit(5i32)));

    let optimized_plan = out.get_optimized_plan();
    println!("Optimized Plan: {optimized_plan:?}")
}
