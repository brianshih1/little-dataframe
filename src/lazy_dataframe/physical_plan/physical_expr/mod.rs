use crate::{dataframe::DataFrame, series::Series};

pub mod binary_expr;
pub mod column;

pub trait PhysicalExpr: Send + Sync {
    fn evaluate(&self, df: &DataFrame) -> Series;
}
