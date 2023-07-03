# Predicate Pushdown

A logical plan in Polars has a tree-like structure. Each node represents a query operation. During execution, the child nodes get executed first before the parent nodes do.

Predicate pushdown is an optimization technique to push the filtering operations (predicates) down the tree, closer to the source. The idea is that the earlier we apply the filter conditions during execution, the less data we have to process.

For example, suppose we have the query:

```rust
df
  .lazy()
  .select([col("A"), col("B")])
  .filter(col("A").gt(lit(1)));
```

Here is the original logical plan:

```rust
FILTER [(col("A")) > (1)] 
	FROM SELECT [col("A"), [(col("B")) + (2)]] 
		FROM DF ["A", "fruits", "B", "cars"]; 
			PROJECT */4 COLUMNS; 
			SELECTION: "None"
```

Here is the optimized plan:

```rust
SELECT [col("A"), [(col("B")) + (2)]]
	FROM DF ["A", "fruits", "B", "cars"]; 
		PROJECT 2/4 COLUMNS; 
		SELECTION: [(col("A")) > (1)]
```

By pushing the predicate, `col("A") > 1` down to the dataframe operation, we avoid having to fetch and process all the rows that don’t fit the condition.

### Algorithm

The core algorithm is really simple. Here is a pseudo-code of the algorithm:

```rust
fn optimize(logical_plan) -> logical_plan {
	let acc_predicates = EMPTY_COLLECTION
	push_down(logical_plan, acc_predicates)
}

fn push_down(logical_plan, acc_predicates) -> logical_plan {
	match logical_plan {
		LogicalPlan::Selection { predicate, input } => {
			acc_predicates.add(predicate)
			push_down(input, acc_predicates)
		},
		
		LogicalPlan::DataFrameScan { df, selection } => {
			LogicalPlan::DataFrameScan {
				df,
				selection: combine_predicates(selection, acc_predicates)
			}
		}
	}
}
```

The `optimize` function takes a logical plan and returns an optimized logical plan. It starts off by initializing an empty collection of predicates. It then recursively calls `push_down` to compute the optimized logical plan for its children.

When `lazy_frame.filter(predicate)` is called, a `LogicalPlan::Selection` is created with the `lazy_frame` becoming the `input` of the `Selection`. When `push_down` encounters a `LogicalPlan::Selection`, the algorithm adds the predicate to the `acc_predicates` and returns `pushdown(input, acc_predicates`. In other words, we removed the `Selection` operation since we pushed down its predicate.

When `df.lazy()` is called, it actually creates a `LogicalPlan::DataFrameScan`. This is the leaf node in a logical plan. When a `push_down` reaches a `DataFrameScan` node, it adds the `acc_predicates` to the `DataFrameScan` node.

If you want to look at Polars code, [here](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L646) is the `optimize` function. We can see that it [calls `push_down`](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L653C18-L653C18) on the root logical plan. Each time the traversal [encounters any `LogicalPlan::Selection`](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L191) it’s [added to the accumulated predicates](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L197) and [replaced with the pushed down version of its child](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L206).

### Pushdown + Join

Join is trickier since it has the left and the right logical plans. For each accumulated predicates, we need to figure out whether to push it to the left plan, right plan, or neither.

Let’s look at an example:

```rust
let df1 = df![
  "foo" => ["abc", "def", "ghi"],
  "idx1" => [0, 0, 1],
  "a" => [1,2, 3]
];
let df2 = df![
  "bar" => [5, 6],
  "idx2" => [0, 1],
  "b" => [1, 2]
];

lf
  .lazy()
  .join(df2.lazy(), [col("idx1")], [col("idx2")], JoinType::Inner)
  .filter(col("bar").eq(lit(5i32)))
  .filter(col("foo").eq(lit("abc")))
  .filter((col("a") + col("b")).gt(lit(12)));
```

In this example, we have a join on `idx1` of `df1` and `idx2` of `df2`. We have 3 filter conditions:

- **predicate 1**: `col(”bar”) = 5`, the `“bar”` column belongs to `df2`
- **predicate 2**: `col("foo") = "abc"`, the `"foo"` column belongs to df1
- **predicate 3**: `col("a") + col("b") > 12`, the `"a"` column belongs to `df1` but the `"b"` column belongs to `df2`.

Here is the logical plan:

```rust
FILTER [([(col("a")) + (col("b"))]) > (12)] 
	FROM FILTER [(col("foo")) == (Utf8(abc))] 
		FROM FILTER [(col("bar")) == (5)] 
			FROM INNER JOIN:
				LEFT PLAN ON: [col("idx1")]
				  DF ["foo", "idx1", "a"]; PROJECT */3 COLUMNS; SELECTION: "None"
				RIGHT PLAN ON: [col("idx2")]
				  DF ["bar", "idx2", "b"]; PROJECT */3 COLUMNS; SELECTION: "None"
			END INNER JOIN
```

Here is the optimized plan:

```rust
FILTER [([(col("a")) + (col("b"))]) > (12)]
FROM
  INNER JOIN:
    LEFT PLAN ON: [col("idx1")]
      DF ["foo", "idx1", "a"];
		    PROJECT */3 COLUMNS;
		    SELECTION: "[(col(\\"foo\\")) == (Utf8(abc))]"

    RIGHT PLAN ON: [col("idx2")]
      DF ["bar", "idx2", "b"];
		    PROJECT */3 COLUMNS;
		    SELECTION: [(col("bar")) == (5)]
END INNER JOIN
```

We can see that `predicate 1` has been pushed down to `df2`, `predicate 2` has been pushed down to `df1`. `predicate 3` has not been pushed down to either since it uses columns from both children.

In simple terms, for each predicate, the algorithm checks if all the columns used in that predicate belongs to either the left or right child. If it is, it can be pushed down. Otherwise, it needs to be applied locally.

[Here](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L392) is the code for how Polars deals with `Join` during `push_down`. [For each predicate](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L407), Polars checks [if it can be pushed down to the left subtree](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L448) or the [right subtree](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L460). [If neither, it is pushed to the local_predicates](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L478). In that case, the local predicates are [wrapped around a Selection logical plan](https://github.com/pola-rs/polars/blob/5ee93f42cc058c8c2ab6b20876ffc5f39e23b665/polars/polars-lazy/polars-plan/src/logical_plan/optimizer/predicate_pushdown/mod.rs#L40) instead of being pushed down.

### Pushdown Boundaries

The algorithm described above mishandles some edge cases. There are scenarios where pushing down predicates is not allowed. In those cases, we need to apply the predicates locally.

```rust
let df = df![
    "vals" => [1, 2, 3, 4, 5]
]
.unwrap();

let lazy_df = df
  .lazy()
  .filter(col("vals").gt(lit(1)))
  // should be > 2
  // if optimizer would combine predicates this would be flawed
  .filter(col("vals").gt(col("vals").min()));
```

This example is borrowed from one of the unit tests in Polars. In this example, we first filter out elements that are ≤ to 1. Then we filter out elements that are ≤ to the minimum of the remaining elements, which is `2`. The result from this would be:

If we combined the two predicates, we would get the following filter operation

```rust
.filter(
	col("vals").gt(lit(1))
		.and(
			col("vals").gt(col("vals").min())
		)
);
```

This would yield the wrong answer because `2` would be in the final output whereas it wouldn’t before the predicates are combined. This is because the first predicate affects the elements that are available to the `min()` operation.

In general, predicates should not be projected downwards if it influences the results of other columns.

In this example, the logical plan is:

```rust
FILTER [(col("vals")) > (col("vals").min())]
	FROM FILTER [(col("vals")) > (1)]
		FROM DF ["vals"]; PROJECT */1 COLUMNS; SELECTION: "None"
```

The optimized plan is:
