use std::sync::Arc;

use crate::{dataframe::DataFrame, physical_plan::physical_expr::PhysicalExpr};

use super::Executor;

pub struct DataFrameScanExec {
    df: Arc<DataFrame>,
    projection: Option<Arc<Vec<String>>>,
    selection: Option<Arc<dyn PhysicalExpr>>,
}

impl Executor for DataFrameScanExec {
    fn execute(&mut self) -> DataFrame {
        todo!()
    }
}
