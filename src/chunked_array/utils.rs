use std::marker::PhantomData;

use arrow2::compute::concatenate::concatenate;

use crate::types::LittleDataType;

use super::ChunkedArray;

/**
 *
 */
pub fn align_chunked_arrays<A, B>(arr1: &ChunkedArray<A>, arr2: &ChunkedArray<B>) -> ChunkedArray<A>
where
    A: LittleDataType,
    B: LittleDataType,
{
    let arr_a_single_chunk = convert_to_single_chunk(arr1);
    let arr_a_primitive_array = &arr_a_single_chunk.chunks[0];
    debug_assert!(arr1.length == 1);

    let mut idx = 0;
    let it = arr2.chunk_length_it();
    let chunks = it
        .map(|len| {
            let slice = arr_a_primitive_array.sliced(idx, len);
            idx += len;
            slice
        })
        .collect();

    ChunkedArray::from_chunks(chunks)
}

pub fn convert_to_single_chunk<T>(chunked_array: &ChunkedArray<T>) -> ChunkedArray<T>
where
    T: LittleDataType,
{
    let list_of_arrow_array = chunked_array
        .chunks
        .iter()
        .map(|v| {
            let arrow_array = &**v;
            arrow_array
        })
        .collect::<Vec<_>>();
    let foo = concatenate(&list_of_arrow_array).unwrap();
    ChunkedArray {
        chunks: vec![foo],
        length: 1,
        phantom: PhantomData,
    }
}
