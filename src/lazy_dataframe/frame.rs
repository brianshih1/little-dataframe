use crate::dataframe::DataFrame;

use super::{expr::Expr, logical_plan::LogicalPlan};

impl DataFrame {
    pub fn lazy(&self) -> LazyFrame {
        todo!()
    }
}

pub struct LazyFrame {
    pub logical_plan: LogicalPlan,
}

impl LazyFrame {
    fn from_logical_plan(plan: LogicalPlan) -> Self {
        LazyFrame { logical_plan: plan }
    }

    fn filter(self, predicate: Expr) -> Self {
        // TODO: Rewrite wildcard, etc
        Self::from_logical_plan(LogicalPlan::Selection {
            input: Box::new(self.logical_plan),
            predicate,
        })
    }
}
