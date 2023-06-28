use crate::{
    dataframe::{groupby::GroupsProxy, DataFrame},
    series::Series,
};

pub mod agg;
pub mod binary_expr;
pub mod column;
pub mod literal;

pub trait PhysicalExpr: Send + Sync {
    fn evaluate(&self, df: &DataFrame) -> Series;

    fn evaluate_for_groups(&self, df: &DataFrame, group_proxy: &GroupsProxy) -> Series;
}
