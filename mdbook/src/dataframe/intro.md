# Dataframe

A [DataFrame](https://pola-rs.github.io/polars/py-polars/html/reference/dataframe/index.html) is a two-dimensional data structure that holds tabular data. Operations that can be executed on DataFrame are similar to what SQL queries do on tables. You can perform joins, groupby, and more queries.

A dataframe is composed of a collection of `Series` of the same length.

```rust
pub struct DataFrame {
    pub columns: Vec<Series>,
		...
}
```

Each Series is just a wrapper around a `ChunkedArray`. In fact, most of the time, implementations of `Series` look something like this:

```rust
pub(crate) struct SeriesWrap<T>(pub T);
```

where T is the ChunkedArray.

Here is how you can create a dataframe:

```rust
let df = df![
    "name" => ["a", "b","a", "b", "c"],
    "points" => [1, 2, 1, 3, 3],
    "age" => [1, 2, 3, 4, 5]
];
```

Now that we know what a dataframe is, let’s look at how `Join` and `Groupby` are implemented.
