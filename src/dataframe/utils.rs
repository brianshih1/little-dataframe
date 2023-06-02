use super::DataFrame;

pub fn split_df(df: &DataFrame, n: usize) -> Vec<DataFrame> {
    let split_size = df.rows_count() / n;
    todo!()
}
