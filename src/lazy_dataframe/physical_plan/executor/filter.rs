use std::sync::Arc;

use crate::{dataframe::DataFrame, lazy_dataframe::physical_plan::physical_expr::PhysicalExpr};

use super::Executor;

pub struct FilterExec {
    pub predicate: Arc<dyn PhysicalExpr>,
    pub input: Box<dyn Executor>,
}

impl Executor for FilterExec {
    fn execute(&mut self) -> DataFrame {
        todo!()
    }
}
