# Mini DataFrame

Mini-dataframe is a toy dataframe library I created for learning purposes. It is built in Rust and uses the [Arrow Columnar Format](https://arrow.apache.org/docs/format/Columnar.html) as the backend. The architecture and API of the library is inspired by [Polars](https://github.com/pola-rs/polars). I reused a lot of algorithms from Polars.

Before starting my project, I wrote a [blog](https://brianshih1.github.io/mini-dataframe/) to deep dive into the algorithms and techniques that Polars uses.

Techniques explored in my toy dataframe library:

- Lazy execution
- Parallel Hash Join
- Parallel Hash Group By
- Predicate pushdown

## Abstractions

### Abstraction 1 - ChunkedArray

I talk about ChunkedArray more deeply [here](https://brianshih1.github.io/mini-dataframe/chunked_array/intro.html).

[ChunkedArray](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-core/src/chunked_array/mod.rs#L148) is the primitive array type in Polars. Each column in a DataFrame (a table) is basically a `ChunkedArray`. You can think of a `ChunkedArray<T>` to be semantically equivalent to `Vec<Option<T>>`.

Operations supported on ChunkedArray:

- **filter**: `(&self, mask: &BooleanChunked) → ChunkedArray<T>`
  - given a mask boolean chunked of the same length, filter the chunked array, only keeping elements with a corresponding true in the mask
- **sort**: `(&self, descending: bool) → ChunkedArray<T>`
  - sorts the ChunkedArray
- **max**: `(&self) → T`
  - returns the max element in the ChunkedArray
- **equal**: `(&self, other: ChunkedArray<T>) → bool`
  - returns true if two ChunkedArray have the same value
- **get_value**: `(&self, index: usize) → Option<AnyValue>`
  - returns the element at index
- **to_vec**: `(&self) → Vec<T>`
  - converts ChunkedArray to vec
- **to_vec_options**: `(&self) → Vec<Option<T>>`
  - converts ChunkedArray to Vec<Option<T>>

In this example, we create an integer chunked array and a mask chunked array and we filter it to get a filtered integer chunked array.

```rust
let chunked = ChunkedArray::new("hello", &vec![1, 5, 7]);
let mask = ChunkedArray::new("foo", &vec![false, true, false]);
let filtered = chunked.filter(&mask);
```

Here is how you can `sort` a `ChunkedArray`:

```rust
let arr = ChunkedArray::new("s", &vec![12, 1, 5, 8]);
let sorted = arr.sort(true);
```

### Abstraction 2 - Series

Series is just a wrapper around `ChunkedArray`. It represents a `column` in a `DataFrame`.

Operations supported on ChunkedArray:

- **vec_hash**: `(&self, hasher: RandomState, buf: &mut Vec<u64>) → ()`
  - given a `buf` with the same length as the Series, compute the hash for each element in the Series
- **vec_hash_combine**: `(&self,hasher: RandomState, buf: &mut Vec<u64>) → ()`
  - given a `buf` with the same length as the Series, combine the existing hash with each element in the Series
- **equal_element**: `(&self, idx_self: usize, other_series: &Series, idx_other: usize) → bool`
  - compares the element in the two Series with respective indices
- **take_indices**: `(&self, indices: &[usize]) -> Series`
  - filters the Series by indices
- **filter**: `(&self, filter: &BooleanChunked) -> Series`
  - filters the Series with a mask
- **agg_min**: `(&self, groups: &GroupsProxy) -> Series`
  - given a GroupProxy, compute the min for each group in the form of a Series

To create a Series you perform:

```rust
Series::new("age", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

### Abstraction 3 - DataFrame

I talk about DataFrame more deeply [here](https://brianshih1.github.io/mini-dataframe/dataframe/intro.html).

A [DataFrame](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html) is a two-dimensional data structure that holds tabular data. It is semantically equivalent to `Vec<Vec<Option<T>>>` where each inner `Vec` represents a column. Each inner `Vec` has the same length, representing the number of rows in the DataFrame.

Operations supported on DataFrame:

- **filter**: `(&self, mask: &BooleanChunked) → DataFrame`
  - given a masked chunk, filter out rows in the DataFrame
- **join**: `(&self, by: Vec<Series>, df2: &DataFrame, df2_by: Vec<Series>, join_type: JoinType) → DataFrame`
  - given the join keys of the two respective DataFrame, perform join on the dataframes
- **compute_group_proxy**: `(&self, by: Vec<Series>) → GroupProxy`
  - given the group keys, compute the GroupProxy. This is a method used to support the LazyFrame

As an example, here is how you can perform an `INNER` join on two dataframes:

```rust
let df1 = DataFrame::new(vec![
    Series::from_vec("name", &vec!["foo", "bar", "baz"]),
    Series::from_vec("points", &vec![0, 10, 20]),
]);

let df2 = DataFrame::new(vec![
    Series::from_vec("name", &vec!["foo", "baz"]),
    Series::from_vec("blocks", &vec![0, 2]),
]);

let joined = df1.inner_join(vec!["name"], &df2, vec!["name"]);
```

### Abstraction 4 - LazyFrame

I talk about LazyFrame more deeply [here](https://brianshih1.github.io/mini-dataframe/lazyframe/intro.html).

LazyFrame is an abstraction that defers the execution until the end which allows Polars to perform query optimizations (e.g. predicate pushdown)

As an example, here is how you perform a `lazy groupby` followed by an `aggregation`.

```rust
let df = DataFrame::new(vec![
    Series::from_vec("name", &vec!["a", "b", "a", "b", "c", "c"]),
    Series::from_vec("points", &vec![1, 2, 3, 2, 1, 0]),
]);

let computed_df = df
    .lazy()
    .groupby(vec![col("name")])
    .agg(vec![col("points").min()])
    .collect();
```

Here is how you perform a lazy filter:

```rust
let expected_df = DataFrame::new(vec![
    Series::from_vec("name", &vec!["foo", "baz"]),
    Series::from_vec("points", &vec![0, 20]),
    Series::from_vec("blocks", &vec![0, 2]),
]);
let res = expected_df
    .lazy()
    .filter(col("points").eq(lit(20)))
    .collect();
```

Here is how you perform a lazy join:

```rust
let df1 = DataFrame::new(vec![
    Series::from_vec("name", &vec!["foo", "bar", "baz"]),
    Series::from_vec("points", &vec![0, 10, 20]),
]);

let df2 = DataFrame::new(vec![
    Series::from_vec("name", &vec!["foo", "baz"]),
    Series::from_vec("blocks", &vec![0, 2]),
]);
let res = df1
    .lazy()
    .join(
        vec![col("name")],
        df2.lazy(),
        vec![col("name")],
        JoinType::Inner,
    )
    .collect();
```

To get the optimized query plan, you can perform:

```rust
let optimized_plan = out.get_optimized_plan();
```
