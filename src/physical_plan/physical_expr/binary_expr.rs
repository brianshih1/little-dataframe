use std::sync::Arc;

use crate::{dataframe::DataFrame, lazy_dataframe::expr::Operator, series::Series};

use super::PhysicalExpr;

pub struct BinaryExpr {
    pub left: Arc<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Arc<dyn PhysicalExpr>,
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, df: DataFrame) -> Series {
        todo!()
    }
}
