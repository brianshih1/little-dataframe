use std::sync::Arc;

use crate::{dataframe::DataFrame, lazy_dataframe::physical_plan::physical_expr::PhysicalExpr};

use super::Executor;

pub struct FilterExec {
    pub predicate: Arc<dyn PhysicalExpr>,
    pub input: Box<dyn Executor>,
}

impl FilterExec {
    pub fn new(predicate: Arc<dyn PhysicalExpr>, input: Box<dyn Executor>) -> Self {
        FilterExec { predicate, input }
    }
}

impl Executor for FilterExec {
    fn execute(&mut self) -> DataFrame {
        let df = self.input.execute();
        let predicate = self.predicate.evaluate(&df);
        df.filter(predicate.bool())
    }
}
