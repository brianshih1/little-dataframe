# LazyFrame

Polars recommends using the Lazy API when dealing with performance-critical code. Unlike the eager API, the lazy API defers the execution until the end which allows Polars to perform query optimizations.

Here is an example of the lazy API:

```rust
df
  .lazy()
  .filter(col("age").gt(lit(25)))
  .groupby(vec!["team"])
  .agg(vec![col("points").sum()])
  .collect();
```

All lazy queries begin with the `lazy()` method. The execution of the query is delayed until `collect` is called. During execution, Polars first rearranges the query with optimizations like predicate pushdown, projection pushdown, type coercion, etc before actually executing the operations. Check out the [list of optimizations](https://pola-rs.github.io/polars-book/user-guide/lazy/optimizations/) used by Polars.

### LazyFrame and LogicalPlan

When `lazy()` is called, the dataframe is converted to a [LazyFrame](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/src/frame/mod.rs#L68).

```rust
pub struct LazyFrame {
    pub logical_plan: LogicalPlan,
		...
}
```

`LazyFrame` is just an abstraction around [LogicalPlan](https://github.com/pola-rs/polars/blob/main/polars/polars-lazy/polars-plan/src/logical_plan/mod.rs#L134). A `LogicalPlan` is an enum of transformations that makes up a query.

```rust
enum LogicalPlan {
		Selection {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
		Join {
        input_left: Box<LogicalPlan>,
        input_right: Box<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
				..
    },
		...
}
```

Operations like `filter`, `select`, `join`, etc creates the LogicalPlans.

```rust
lf.filter(col("age").gt(lit(25)))
```

For example, here is a simplified implementation of `filter`:

```rust
impl LazyFrame {
		pub fn filter(self, predicate: Expr) -> LazyFrame {
			let lp = LogicalPlan::Selection {
				input: Box::new(self.logical_plan),
				predicate
			};
			LazyFrame::from_logical_plan(lp)
		}
}
```

If you want to look at the `logical_plan` constructed by your query, you can perform:

```rust
lazy_frame.logical_plan.describe()
```

### Optimization

When [collect](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/src/frame/mod.rs#L547) is called, it will optimize and [rearrange the logical plan](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/src/frame/mod.rs#L501). The optimized `logical_plan` will then be [converted](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/src/frame/mod.rs#L525) to a `physical_plan`. A physical plan is an `[Executor](<https://github.com/pola-rs/polars/blob/main/polars/polars-lazy/src/physical_plan/executors/executor.rs#L10>)` that can generate a dataframe. For example, here is the [physical plan](https://github.com/pola-rs/polars/blob/main/polars/polars-lazy/src/physical_plan/executors/filter.rs#L10) for filter and here is the function that converts a [logical plan](https://github.com/pola-rs/polars/blob/main/polars/polars-lazy/src/physical_plan/planner/lp.rs#L142) to a physical plan.

If you want to look at the optimized logical plan, you can perform:

```rust
lazy_frame.describe_optimized_plan()
```

You can also turn each optimizers on and off like this:

```rust
lf.with_predicate_pushdown(true)
        .describe_optimized_plan()
```

In the remaining sections, we will deep dive into a couple of optimizations Polars uses.
