use super::DataFrame;

pub fn split_df(df: &DataFrame, n: usize) -> Vec<DataFrame> {
    let split_size = df.rows_count() / n;

    let chunk_length = std::cmp::max(split_size, 1);
    let mut frames = Vec::with_capacity(n);
    println!("chunk_length: {}", chunk_length);
    println!("split_size: {}", split_size);
    for i in 0..n {
        let offset = i * chunk_length;
        if i == n - 1 {
            let sub_df = df.slice(offset, df.rows_count() - offset);
            frames.push(sub_df);
        } else {
            if offset >= df.rows_count() {
                break;
            }

            let sub_df = df.slice(
                offset,
                std::cmp::min(df.rows_count() - offset, chunk_length),
            );
            frames.push(sub_df);
        }
    }
    frames
}
