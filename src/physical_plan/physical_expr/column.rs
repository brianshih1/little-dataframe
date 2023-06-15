use std::sync::Arc;

use crate::{dataframe::DataFrame, series::Series};

use super::PhysicalExpr;

pub struct ColumnExpr {
    pub col_name: Arc<str>,
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(&self, df: DataFrame) -> Series {
        todo!()
    }
}
