use std::sync::Arc;

use crate::{dataframe::DataFrame, lazy_dataframe::physical_plan::physical_expr::PhysicalExpr};

use super::Executor;

pub struct DataFrameScanExec {
    df: Arc<DataFrame>,
    projection: Option<Arc<Vec<String>>>,
    selection: Option<Arc<dyn PhysicalExpr>>,
}

impl DataFrameScanExec {
    pub fn new(
        df: Arc<DataFrame>,
        projection: Option<Arc<Vec<String>>>,
        selection: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        DataFrameScanExec {
            df,
            projection,
            selection,
        }
    }
}

impl Executor for DataFrameScanExec {
    fn execute(&mut self) -> DataFrame {
        let df = std::mem::take(&mut self.df);
        let mut df = Arc::try_unwrap(df).unwrap_or_else(|df| (*df).clone());

        if let Some(projection) = &self.projection {
            df = df.select(projection.iter())
        }

        let pred = self.selection.as_ref().map(|s| s.evaluate(&df));

        if let Some(pred) = pred {
            df = df.filter(pred.bool());
        };
        df
    }
}
