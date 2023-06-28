use std::sync::Arc;

use crate::{
    dataframe::{groupby::GroupsProxy, DataFrame},
    series::Series,
};

use super::PhysicalExpr;

pub struct ColumnExpr {
    pub col_name: Arc<str>,
}

impl ColumnExpr {
    pub fn new(col_name: Arc<str>) -> Self {
        ColumnExpr { col_name }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(&self, df: &DataFrame) -> Series {
        df.column(&self.col_name)
    }

    fn evaluate_for_groups(&self, df: &DataFrame, group_proxy: &GroupsProxy) -> Series {
        todo!()
    }
}
