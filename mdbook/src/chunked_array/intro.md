# ChunkedArray

[ChunkedArray](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-core/src/chunked_array/mod.rs#L148) is the primitive array type in Polars. Each column in a DataFrame (a table) is basically a `ChunkedArray`.

You can create a ChunkedArray like this:

```rust
let c1 = ChunkedArray::new("foo", [1, 2, 3, 4, 5]);
```

`ChunkedArray` is semantically equivalent to a single array, but each `ChunkedArray` is actually composed of a list of `Arrow2` arrays,  the Rust implementation of the [Arrow memory layout](https://arrow.apache.org/docs/dev/format/Columnar.html). This list is stored in the `chunks` property as shown below:

```rust
pub struct ChunkedArray<T: PolarsDataType> {
    pub(crate) chunks: Vec<ArrayRef>,
    ...
}

pub type ArrayRef = Box<dyn Array>; // Arrow2 Array
```

There are a few reasons behind storing data in chunks of Arrow2 arrays.

Firstly, performing `append` to a `ChunkedArray` is cheap because the new array can just be added to the `chunks` property instead of causing a reallocation of the entire array.

Secondly, as stated in [Apache Arrow’s documentation](https://arrow.apache.org/docs/cpp/conventions.html), Arrow arrays are immutable. Therefore, it’s possible to have multiple zero copy views (or slices) of the array. This makes operations such as cloning or slicing cheap since it just creates another pointer to the same allocation.

**Type**

Each element in the `ChunkedArray` must have the same type. This is because the Arrow Columnar Format specifies that all elements in the Array must have the same type. This makes it SIMD and vectorization-friendly. Supported data types in Polars are listed [here](https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/datatypes/dtype.rs#L5).

**Validity**

In Apache Arrow, any value in an array [may be semantically null](https://arrow.apache.org/docs/format/Columnar.html#validity-bitmaps). Each array has a validity bitmap that specifies whether the value is null.

### Primitive Array

We mentioned that Chunked Array is just an array of `Arrow` arrays. Let’s actually look at what Arrow arrays are. [PrimitiveArray](https://github.com/jorgecarleitao/arrow2/blob/eed5ebb2b0d18dfbcce363f5d212410f52a49333/src/array/primitive/mod.rs#L52) is the Rust implementation of the Apache Arrow array. Conceptually, `PrimitiveArray` is an immutable `Vec<Option<T>>`.

```rust
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
}
```

[Buffer](https://github.com/jorgecarleitao/arrow2/blob/eed5ebb2b0d18dfbcce363f5d212410f52a49333/src/buffer/immutable.rs#L8) is a data structure that can be shared across threads and can be [sliced and cloned in O(1)](https://github.com/jorgecarleitao/arrow2/blob/eed5ebb2b0d18dfbcce363f5d212410f52a49333/src/buffer/immutable.rs#L13).

The `validity` property stores whether each element in the array is null or not in a [BitMap](https://github.com/jorgecarleitao/arrow2/blob/eed5ebb2b0d18dfbcce363f5d212410f52a49333/src/bitmap/immutable.rs#L13), which is semantically equivalent to `Arc<Vec<bool>>`.

In other words, to initialize a `PrimitiveArray`, you need the data type, the raw values, and an array that specifies whether each element is null. For example:

```rust
let primitive_array = PrimitiveArray::new(
	ArrowDataType::Float32, 
	vec![12, 15, 16].into(), 
	Some(vec![true, false, true].into())
);
```

Now that we know what a ChunkedArray is, let’s look at how some of its operations are implemented.
