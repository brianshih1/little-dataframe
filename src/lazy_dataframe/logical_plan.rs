use std::sync::Arc;

use crate::{
    core::schema::Schema,
    dataframe::{join::JoinType, DataFrame},
};

use super::expr::Expr;

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
