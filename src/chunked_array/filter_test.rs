#[cfg(test)]
mod filter {
    use std::time::Instant;

    use rand::Rng;

    use crate::{
        chunked_array::{
            builder::NewFrom, filter::ChunkedArrayFilter, test_utils::AssertUtils, ChunkedArray,
        },
        random::{
            generate_boolean_array_all_false, generate_boolean_array_all_true,
            generate_random_boolean_array, generate_random_i32_array,
        },
    };

    use super::dumb_filter;

    #[test]
    fn filter() {
        let chunked = ChunkedArray::new("hello", &vec![1, 5, 7]);
        let mask = ChunkedArray::new("foo", &vec![false, true, false]);
        let filtered = chunked.filter(&mask);
        filtered.equals_lists(&[&[5]]);
        let mut rng = rand::thread_rng();
    }

    #[ignore]
    #[test]
    fn benchmark() {
        let size = 10000;
        let arr = generate_random_i32_array(size);
        // let mask = generate_random_boolean_array(size);
        // let mask = generate_boolean_array_all_true(size);
        let mask = generate_boolean_array_all_false(size);

        let chunked = ChunkedArray::new("hello", &arr);
        let mask_chunked = ChunkedArray::new("foo", &mask);

        // measure dumb filter
        let start_time = Instant::now();
        dumb_filter(&arr, &mask);
        let end_time = Instant::now();
        let duration = end_time - start_time;

        // measure chunked filter
        let start_time = Instant::now();
        chunked.filter(&mask_chunked);
        let end_time = Instant::now();
        let duration2 = end_time - start_time;

        println!(
            "Dumb duration: {:?}. ChunkedArray duration: {:?}",
            duration, duration2
        );
    }
}

fn dumb_filter<T>(list: &Vec<T>, mask: &Vec<bool>) -> Vec<T>
where
    T: Clone,
{
    let mut ret_list = Vec::with_capacity(list.len());
    list.iter().zip(mask.iter()).for_each(|(item, pred)| {
        if *pred {
            ret_list.push(item.clone())
        }
    });
    ret_list
}
