use std::sync::Arc;

use crate::{
    dataframe::join::JoinType, lazy_dataframe::physical_plan::physical_expr::PhysicalExpr,
};

use super::Executor;

pub struct JoinExec {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    left_on: Vec<Arc<dyn PhysicalExpr>>,
    right_on: Vec<Arc<dyn PhysicalExpr>>,
    join_type: JoinType,
}

impl Executor for JoinExec {
    fn execute(&mut self) -> crate::dataframe::DataFrame {
        todo!()
    }
}
