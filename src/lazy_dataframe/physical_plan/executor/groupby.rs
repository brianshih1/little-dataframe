use std::sync::Arc;

use crate::{
    dataframe::DataFrame, lazy_dataframe::physical_plan::physical_expr::PhysicalExpr,
    series::Series,
};

use super::Executor;

pub struct GroupByExec {
    pub input: Box<dyn Executor>,
    pub keys: Vec<Arc<dyn PhysicalExpr>>,
    pub agg: Vec<Arc<dyn PhysicalExpr>>,
}

impl Executor for GroupByExec {
    fn execute(&mut self) -> DataFrame {
        let df = self.input.execute();
        let by = self
            .keys
            .iter()
            .map(|expr| expr.evaluate(&df))
            .collect::<Vec<Series>>();
        let group_proxy = df.compute_group_proxy(by.clone());

        let mut columns_selected = by
            .iter()
            .map(|col| compute_key(col, &group_proxy.first))
            .collect::<Vec<Series>>();
        let columns_aggregated = self
            .agg
            .iter()
            .map(|expr| expr.evaluate_for_groups(&df, &group_proxy))
            .collect::<Vec<Series>>();
        columns_selected.extend(columns_aggregated);
        DataFrame::new(columns_selected)
    }
}

fn compute_key(series: &Series, indices: &Vec<u32>) -> Series {
    // Extracts the series to only keep the elements in the indices
    todo!()
}
