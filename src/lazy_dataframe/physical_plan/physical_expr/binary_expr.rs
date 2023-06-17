use std::sync::Arc;

use crate::{dataframe::DataFrame, lazy_dataframe::expr::Operator, series::Series};

use super::PhysicalExpr;

pub struct BinaryExpr {
    pub left: Arc<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    pub fn new(left: Arc<dyn PhysicalExpr>, op: Operator, right: Arc<dyn PhysicalExpr>) -> Self {
        BinaryExpr { left, op, right }
    }
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, df: DataFrame) -> Series {
        todo!()
    }
}
