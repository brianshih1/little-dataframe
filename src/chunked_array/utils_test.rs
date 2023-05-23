#[cfg(test)]
mod test {
    mod convert_to_single_chunk {
        use crate::chunked_array::{
            builder::NewFrom, utils::convert_to_single_chunk, ChunkedArray,
        };

        #[test]
        fn single_chunk() {
            let list = ChunkedArray::from_lists("", vec![&vec![1, 2, 3], &vec![4, 5, 6]]);
            let single_chunk = convert_to_single_chunk(&list);
            assert_eq!(single_chunk.chunks.len(), 1);
            let chunks_length = single_chunk.chunk_length_it().collect::<Vec<usize>>();
            assert_eq!(chunks_length, vec![6]);
        }
    }
}
