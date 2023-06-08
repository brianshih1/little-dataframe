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
