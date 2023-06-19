use std::sync::Arc;

use crate::dataframe::join::JoinType;

use super::{
    expr::Expr,
    logical_plan::{det_join_schema, LogicalPlan},
};

pub struct LogicalPlanBuilder(LogicalPlan);

impl From<LogicalPlan> for LogicalPlanBuilder {
    fn from(lp: LogicalPlan) -> Self {
        LogicalPlanBuilder(lp)
    }
}

impl LogicalPlanBuilder {
    pub fn from_logical_plan(plan: LogicalPlan) -> Self {
        Self(plan)
    }

    pub fn build(self) -> LogicalPlan {
        self.0
    }

    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Selection {
            input: Box::new(self.0),
            predicate,
        }
        .into()
    }

    pub fn join(
        self,
        left_on: Vec<Expr>,
        right_df: LogicalPlan,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> Self {
        let left_schema = Arc::new(self.0.schema());
        let right_schema = Arc::new(right_df.schema());
        let joined_schema =
            det_join_schema(&left_schema, &right_schema, &left_on, &right_on, &join_type);
        LogicalPlan::Join {
            left: Box::new(self.0),
            right: Box::new(right_df),
            left_on,
            right_on,
            join_type,
            schema: joined_schema,
        }
        .into()
    }
}
