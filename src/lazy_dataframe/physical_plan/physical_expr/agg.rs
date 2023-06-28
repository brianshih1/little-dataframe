use std::sync::Arc;

use crate::{
    dataframe::{groupby::GroupsProxy, DataFrame},
    series::Series,
};

use super::PhysicalExpr;

pub enum AggregationExpr {
    Min(Arc<dyn PhysicalExpr>),
}

impl PhysicalExpr for AggregationExpr {
    fn evaluate(&self, df: &DataFrame) -> Series {
        todo!()
    }

    fn evaluate_for_groups(&self, df: &DataFrame, group_proxy: &GroupsProxy) -> Series {
        match self {
            AggregationExpr::Min(agg) => {
                // TODO: This should be evaluate_groups.
                // But for the MVP, let's just only support col(...).agg(...)
                let series = agg.evaluate(df);
                series.agg_min(group_proxy)
            }
        }
    }
}
