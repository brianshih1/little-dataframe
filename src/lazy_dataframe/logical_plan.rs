use std::sync::Arc;

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
    // TODO: Projection
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::Selection { input, predicate } => input.schema(),
            LogicalPlan::DataFrameScan { schema, .. } => schema.clone(),
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
