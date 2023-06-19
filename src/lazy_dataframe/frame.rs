use crate::dataframe::{join::JoinType, DataFrame};

use super::{
    aexpr::AExpr,
    alogical_plan::ALogicalPlan,
    arena::{Arena, Node},
    expr::Expr,
    logical_plan::LogicalPlan,
    logical_plan_builder::LogicalPlanBuilder,
};

impl DataFrame {
    pub fn lazy(&self) -> LazyFrame {
        todo!()
    }
}

pub struct LazyFrame {
    pub logical_plan: LogicalPlan,
}

impl LazyFrame {
    pub fn from_logical_plan(plan: LogicalPlan) -> Self {
        LazyFrame { logical_plan: plan }
    }

    pub fn get_plan_builder(self) -> LogicalPlanBuilder {
        LogicalPlanBuilder::from_logical_plan(self.logical_plan)
    }

    pub fn filter(self, predicate: Expr) -> Self {
        // TODO: Rewrite wildcard, etc
        Self::from_logical_plan(self.get_plan_builder().filter(predicate).build())
    }

    pub fn join(
        self,
        left_on: Vec<Expr>,
        right_df: LazyFrame,
        right_on: Vec<Expr>,
        join_type: JoinType,
    ) -> Self {
        Self::from_logical_plan(
            self.get_plan_builder()
                .join(left_on, right_df.logical_plan, right_on, join_type)
                .build(),
        )
    }

    pub fn optimize_from_scratch(&self) {
        todo!()
    }
}

pub fn optimize(
    logical_plan: LogicalPlan,
    alp_arena: &mut Arena<ALogicalPlan>,
    expr_arena: &mut Arena<AExpr>,
) -> Node {
    todo!()
}
