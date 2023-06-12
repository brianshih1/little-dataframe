use crate::{chunked_array::builder::NewFrom, dataframe::DataFrame, series::Series};

use super::join_group_indices;

#[test]
fn join_group_indices_test() {
    let arr: Vec<(Vec<u32>, Vec<Vec<u32>>)> = vec![
        (vec![1, 2, 3], vec![vec![11, 111], vec![22], vec![33]]),
        (vec![4, 5, 6], vec![vec![44], vec![55, 555], vec![]]),
    ];

    let output = join_group_indices(arr);
    assert_eq!(
        output,
        (
            vec![1, 2, 3, 4, 5, 6],
            vec![
                vec![11, 111],
                vec![22],
                vec![33],
                vec![44],
                vec![55, 555],
                vec![]
            ]
        )
    )
}

#[test]
fn test_compute_group_proxy() {
    let df = DataFrame::new(vec![
        Series::from_vec("name", &vec!["a", "b", "a", "b", "c"]),
        Series::from_vec("points", &vec![1, 2, 1, 3, 3]),
    ]);
    let group_proxy = df.compute_group_proxy(vec![Series::from_vec(
        "name",
        &vec!["a", "b", "a", "b", "c"],
    )]);
    println!("Proxy: {:?}", group_proxy);
}
