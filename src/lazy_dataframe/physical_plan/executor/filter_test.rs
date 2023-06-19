use std::sync::Arc;

use crate::{
    chunked_array::builder::NewFrom,
    dataframe::DataFrame,
    lazy_dataframe::{
        expr::{col, Expr},
        lit::{self, lit},
        physical_plan::physical_expr::column,
    },
    series::Series,
};

#[test]
fn test_filter() {
    let expected_df = DataFrame::new(vec![
        Series::from_vec("name", &vec!["foo", "baz"]),
        Series::from_vec("points", &vec![0, 20]),
        Series::from_vec("blocks", &vec![0, 2]),
    ]);
    let res = expected_df
        .lazy()
        .filter(col("points").eq(lit(20)))
        .collect();
}
