# Sort

[Sorting](https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/chunked_array/ops/sort/mod.rs#L314) in Polars is a relatively straightforward operation. The main optimization it performs is that it parallelizes sorting the data with Rayon’s [par_sort_unstable_by](https://docs.rs/rayon/latest/rayon/slice/trait.ParallelSliceMut.html#method.par_sort_unstable_by) method.

The current implementation of `par_sort_unstable_by` uses quicksort. Quicksort can be broken down into two stages - partitioning the array into two halves and then recursively performing quicksort on both halves. The partitioning phase needs to be sequential, but the recursive calls can be performed in parallel.

[Here](https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/chunked_array/ops/sort/mod.rs#L63) is Polars’ code:

```rust
POOL.install(|| match descending {
    true => slice.par_sort_unstable_by(descending_order_fn),
    false => slice.par_sort_unstable_by(ascending_order_fn),
})
```

### Dealing With Validity with Branchless Code

Earlier, we mentioned that each value in Arrow2’s array can be null. This slightly complicates the implementation because we need to keep track of which values are null and which values aren’t during sorting. This may involve a lot of conditional code branching.

Luckily, Arrow2 stores the raw values and the validity bitmap (the bitmap stores information about whether or not the value is null) separately. This means that we can just ignore the null values until the end of the operation. Let’s look at what I mean by that.

When Polars performs sorting on arrays with null, it places all the nulls at the end of the sorted array (or front if the user chooses to do so). Polars first [filters out](https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/chunked_array/ops/sort/mod.rs#L200) the non-null values and performs parallel sorting on them. It then fills out the null values with the default value as follows:

```rust
let iter = std::iter::repeat(T::Native::default()).take(null_count);
vals.extend(iter);
```

Computing the final validity Bitmap is also straightforward. Since all the `null` values are at the front of the array, the validity Bitmap is `true` for the first `len - null_count` elements and `false` for the final `null_count` elements.

[Here](https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-core/src/chunked_array/ops/sort/mod.rs#L218) is the code:

```rust
let mut validity = MutableBitmap::with_capacity(len);
validity.extend_constant(len - null_count, true);
validity.extend_constant(null_count, false);
```

In Ritchie’s [blog](https://www.ritchievink.com/blog/2021/02/28/i-wrote-one-of-the-fastest-dataframe-libraries/), he points out that by storing the validity bitmap separately from the buffer (raw value), it is easy to write branchless code by ignoring the null buffer during the operation. Then when the operation is finished, the null bit buffer is just copied to the new array. The sort algorithm is a perfect example of what Ritchie meant.
