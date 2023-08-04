# What is Polars?

Before we go into the implementation details of Polars, let’s quickly look at what Polars is.

Polars is a super fast DataFrame library for Rust and Python. It exposes a set of query APIs that are similar to Pandas.

For example, here is an example provided by Polars to load a parquet file, and perform operations such as `groupby`, `agg` and `join`:

```rust
let lf1 = LazyFrame::scan_parquet("myfile_1.parquet", Default::default())?
    .groupby([col("ham")])
    .agg([
        // expressions can be combined into powerful aggregations
        col("foo")
            .sort_by([col("ham").rank(Default::default(), None)], [false])
            .last()
            .alias("last_foo_ranked_by_ham"),
        // every expression runs in parallel
        col("foo").cummin(false).alias("cumulative_min_per_group"),
        // every expression runs in parallel
        col("foo").reverse().implode().alias("reverse_group"),
    ]);

let lf2 = LazyFrame::scan_parquet("myfile_2.parquet", Default::default())?
    .select([col("ham"), col("spam")]);

let df = lf1
    .join(lf2, [col("reverse")], [col("foo")], JoinArgs::new(JoinType::Left))
    // now we finally materialize the result.
    .collect()?;
```

Polars uses [Apache Arrow](https://arrow.apache.org/) as its Memory Model. Polars also uses techniques such as SIMD instructions, parallelization, query optimization, and many other techniques to have a lightning-fast performance.

### Apache Arrow

The best way to understand Apache Arrow is to check out its [official documentation](https://arrow.apache.org/). To summarize, it is an in-memory columnar format for representing tabular data.

The columnar format is beneficial for computations because it enables vectorization using SIMD (Single Instruction, Multiple Data). Apache Arrow also provides zero-copy reads which makes copying data around cheap.

I personally haven’t had the time to dig into the details of the Apache Arrow design but I definitely plan to one day!
