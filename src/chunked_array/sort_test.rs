use std::time::Instant;

use crate::{chunked_array::sort::sort_list, random::generate_random_i32_array};

#[ignore]
#[test]
fn sort_vs_parallel_sort() {
    let size = 100000;
    let mut arr = generate_random_i32_array(size);
    // measure dumb sort
    let start_time = Instant::now();
    arr.sort();
    let end_time = Instant::now();
    let duration = end_time - start_time;

    // measure parallel sort
    let start_time = Instant::now();
    sort_list(&mut arr, true, |a, b| a.cmp(b), |a, b| a.cmp(b));
    let end_time = Instant::now();
    let duration2 = end_time - start_time;

    println!(
        "Dumb sort duration: {:?}. Parallel sort duration: {:?}",
        duration, duration2
    );
}

mod sort_i32 {
    use std::time::Instant;

    use crate::{
        chunked_array::{
            builder::NewFrom,
            sort::{sort_list, ChunkedSort},
            test_utils::AssertUtils,
            ChunkedArray,
        },
        random::generate_random_i32_array,
    };

    #[test]
    fn sort_not_null_i32() {
        let arr = ChunkedArray::new("s", &vec![12, 1, 5, 8]);
        let sorted = arr.sort(true);
        println!("arr: {:?}", &sorted);
        assert_eq!(sorted.to_vec(), vec![1, 5, 8, 12]);
    }

    #[test]
    fn sort_vec() {
        let mut first = vec![12, 1, 5];
        sort_list(&mut first, true, |a, b| b.cmp(a), |a, b| a.cmp(b));
        assert_eq!(&first, &vec![12, 5, 1]);

        let mut second = vec![30, 3, 8, 5, 19, 28];
        sort_list(&mut second, false, |a, b| b.cmp(a), |a, b| a.cmp(b));
        assert_eq!(&second, &vec![3, 5, 8, 19, 28, 30]);
    }
}
