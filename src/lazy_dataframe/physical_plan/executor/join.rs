use std::sync::Arc;

use crate::{
    dataframe::{join::JoinType, DataFrame},
    lazy_dataframe::physical_plan::physical_expr::PhysicalExpr,
    series::Series,
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
    fn execute(&mut self) -> DataFrame {
        let left_df = self.left.execute();
        let right_df = self.right.execute();
        let left_on = self
            .left_on
            .iter()
            .map(|expr| expr.evaluate(&left_df))
            .collect::<Vec<Series>>();
        let right_on = self
            .right_on
            .iter()
            .map(|expr| expr.evaluate(&right_df))
            .collect::<Vec<Series>>();

        match self.join_type {
            JoinType::Left => todo!(),
            JoinType::Inner => left_df.join(left_on, &right_df, right_on, JoinType::Inner),
            JoinType::Outer => todo!(),
        }
    }
}
