use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use ahash::{HashSet, HashSetExt};

use crate::{
    core::schema::{Schema, SchemaRef},
    dataframe::{join::JoinType, DataFrame},
};

use super::{aexpr::expr_to_aexpr, arena::Arena, expr::Expr};

#[derive(Clone)]
// Polars LogicalPlan: https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-lazy/polars-plan/src/logical_plan/mod.rs
pub enum LogicalPlan {
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_on: Vec<Expr>,
        right_on: Vec<Expr>,
        join_type: JoinType,
        schema: Arc<Schema>,
    },
    // Basically a filter
    Selection {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    DataFrameScan {
        df: Arc<DataFrame>,
        projection: Option<Arc<Vec<String>>>,
        selection: Option<Expr>,
        schema: Arc<Schema>,
    },
    GroupBy {
        keys: Vec<Expr>,
        agg: Vec<Expr>,
        input: Box<LogicalPlan>,
    },
    // TODO: Projection
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::Selection { input, predicate } => input.schema(),
            LogicalPlan::DataFrameScan { schema, .. } => schema.clone(),
            LogicalPlan::GroupBy { keys, agg, .. } => todo!(),
        }
    }
}

pub fn det_join_schema(
    schema_left: &SchemaRef,
    schema_right: &SchemaRef,
    _left_on: &[Expr],
    right_on: &[Expr],
    _join_type: &JoinType,
) -> SchemaRef {
    // TODO: with capacity
    let mut schema = Schema::new();
    schema_left.iter().for_each(|(name, dtype)| {
        schema.with_column(name.clone(), dtype.clone());
    });

    let mut right_join_keys = HashSet::with_capacity(right_on.len());
    let mut expr_arena = Arena::new();
    right_on.iter().for_each(|key| {
        let aexpr = expr_to_aexpr(key.clone(), &mut expr_arena);
        let field = expr_arena.get(aexpr).to_field(&schema_right);
        right_join_keys.insert(field.name);
    });
    schema_right.iter().for_each(|(name, dtype)| {
        if !right_join_keys.contains(name) {
            schema.with_column(name.clone(), dtype.clone());
        }
    });
    // TODO: Join Types
    Arc::new(schema)
}

impl LogicalPlan {
    fn _fmt(&self, f: &mut Formatter, indent: usize) -> fmt::Result {
        let next_indent = indent + 3;
        match self {
            LogicalPlan::Join {
                left,
                right,
                left_on,
                right_on,
                join_type,
                schema,
            } => {
                write!(f, "{:indent$}{join_type:?} JOIN:", "")?;
                write!(f, "\n{:indent$}LEFT ON: {left_on:?}", "")?;
                left._fmt(f, next_indent)?;
                write!(f, "\n{:indent$}RIGHT ON: {right_on:?}", "")?;
                right._fmt(f, next_indent)?;
                write!(f, "\n{:indent$}END {join_type:?} JOIN", "")
            }
            LogicalPlan::Selection { input, predicate } => {
                write!(f, "{:indent$}FILTER {predicate:?} FROM", "")?;
                input._fmt(f, indent)
            }
            LogicalPlan::DataFrameScan {
                df,
                projection,
                selection,
                schema,
            } => {
                write!(f, "{:indent$}DF:", "")?;
                write!(f, "\n{:indent$} PROJECT: {projection:?}", "")?;
                write!(f, "\n{:indent$} SELECTION: {selection:?}", "")?;
                write!(f, "\n{:indent$} SCHEMA: {schema:?}", "")
            }
            LogicalPlan::GroupBy { keys, agg, input } => {
                write!(f, "{:indent$}GROUPBY:", "")?;

                write!(f, "\n{:indent$} KEYS: {keys:?}", "")?;
                write!(f, "\n{:indent$} BY: {agg:?}", "");
                write!(f, "\n{:indent$} INPUT: {input:?}", "")
            }
        }
    }
}

impl Debug for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        self._fmt(f, 0)
    }
}
