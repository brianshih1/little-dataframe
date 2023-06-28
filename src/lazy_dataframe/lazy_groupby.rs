use super::{expr::Expr, frame::LazyFrame, logical_plan::LogicalPlan};

pub struct LazyGroupBy {
    input: LogicalPlan,
    by: Vec<Expr>,
}

impl LazyGroupBy {
    pub fn new(input: LogicalPlan, by: Vec<Expr>) -> Self {
        LazyGroupBy { input, by }
    }
}

impl LazyGroupBy {
    pub fn agg(self, agg: Vec<Expr>) -> LazyFrame {
        let lp = LogicalPlan::GroupBy {
            keys: self.by,
            agg,
            input: Box::new(self.input),
        };
        LazyFrame::from_logical_plan(lp)
    }
}
