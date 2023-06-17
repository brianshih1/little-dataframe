use std::sync::Arc;

use crate::{
    chunked_array::chunk_compare::ChunkCompare,
    dataframe::DataFrame,
    lazy_dataframe::expr::Operator,
    series::{constructor::IntoSeries, Series},
};

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
    fn evaluate(&self, df: &DataFrame) -> Series {
        let left = self.left.evaluate(df);
        let right = self.right.evaluate(df);
        match self.op {
            Operator::And => todo!(),
            Operator::Or => todo!(),
            Operator::Eq => {
                let boolean_chunk = left.equal(&right);
                boolean_chunk.into_series()
            }
        }
    }
}
