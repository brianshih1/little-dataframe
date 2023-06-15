use crate::{dataframe::DataFrame, series::Series};

pub mod binary_expr;
pub mod column;
pub mod filter;

pub trait PhysicalExpr: Send + Sync {
    fn evaluate(&self, df: DataFrame) -> Series;
}