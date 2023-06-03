use crate::chunked_array::builder::NewFrom;
use crate::series::Series;

use super::utils::split_df;
use super::DataFrame;

#[test]
fn test_split_df() {
    let df = DataFrame::new(vec![
        Series::new("age", &vec![0, 1, 2, 3, 4]),
        Series::new("name", &vec!["a", "b", "c", "d", "e"]),
    ]);
    split_df(&df, 3);
    println!("Df: {:?}", df);
}
