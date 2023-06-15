use crate::dataframe::DataFrame;

use super::{
    aexpr::AExpr,
    alogical_plan::ALogicalPlan,
    arena::{Arena, Node},
    expr::Expr,
    logical_plan::LogicalPlan,
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

    pub fn filter(self, predicate: Expr) -> Self {
        // TODO: Rewrite wildcard, etc
        Self::from_logical_plan(LogicalPlan::Selection {
            input: Box::new(self.logical_plan),
            predicate,
        })
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
