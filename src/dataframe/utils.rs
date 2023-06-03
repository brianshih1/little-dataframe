use super::DataFrame;

pub fn split_df(df: &DataFrame, n: usize) -> Vec<DataFrame> {
    let split_size = df.rows_count() / n;

    let chunk_length = std::cmp::max(split_size, 1);
    let mut frames = Vec::with_capacity(n);
    for i in 0..n {
        let offset = i * chunk_length;
        let sub_df = df.slice(offset, chunk_length);
        frames.push(sub_df);
    }
    frames
}
