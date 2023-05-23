#[cfg(test)]
mod aggregate {
    use std::time::Instant;

    use crate::{
        chunked_array::{builder::NewFrom, ChunkedArray},
        random::generate_random_i32_array,
    };

    #[test]
    fn max() {
        let chunked = ChunkedArray::new("hello", &vec![150, 200, 5, 7]);
        let max = chunked.max();
        assert_eq!(max, Some(200));
    }
    #[ignore]
    #[test]
    fn benchmark() {
        let size = 1000000;
        let arr = generate_random_i32_array(size);

        let chunked = ChunkedArray::new("hello", &arr);

        // measure dumb filter
        let start_time = Instant::now();
        chunked.max();
        let end_time = Instant::now();
        let duration = end_time - start_time;

        // measure chunked filter
        let start_time = Instant::now();
        dumb_max(&arr);
        let end_time = Instant::now();
        let duration2 = end_time - start_time;

        println!(
            "Dumb duration: {:?}. ChunkedArray duration: {:?}",
            duration, duration2
        );
    }

    fn dumb_max(arr: &Vec<i32>) -> Option<&i32> {
        arr.iter().max()
    }
}
