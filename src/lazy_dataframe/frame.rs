use std::sync::Arc;

use crate::{
    dataframe::{join::JoinType, DataFrame},
    lazy_dataframe::alogical_plan::logical_to_alp,
};

use super::{
    aexpr::AExpr,
    alogical_plan::{alp_node_to_physical_plan, ALogicalPlan},
    arena::{Arena, Node},
    expr::Expr,
    lazy_groupby::LazyGroupBy,
    logical_plan::LogicalPlan,
    logical_plan_builder::LogicalPlanBuilder,
    optimizer::predicate_pushdown::PredicatePushdown,
    physical_plan::executor::Executor,
};

impl DataFrame {
    pub fn lazy(self) -> LazyFrame {
        let schema = self.schema();
        LazyFrame::from_logical_plan(LogicalPlan::DataFrameScan {
            df: Arc::new(self),
            projection: None,
            selection: None,
            schema: Arc::new(schema),
        })
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

    pub fn groupby(self, by: Vec<Expr>) -> LazyGroupBy {
        LazyGroupBy::new(self.logical_plan, by)
    }

    pub fn optimize_with_scratch(
        self,
        alp_arena: &mut Arena<ALogicalPlan>,
        expr_arena: &mut Arena<AExpr>,
    ) -> Node {
        let node = logical_to_alp(self.logical_plan, expr_arena, alp_arena);

        let predicate_pushdown = PredicatePushdown::new();
        let alp = alp_arena.take(node);
        let new_alp = predicate_pushdown.optimize(alp, alp_arena, expr_arena);
        alp_arena.replace(node, new_alp);
        node
    }

    pub fn get_optimized_plan(self) -> LogicalPlan {
        let mut expr_arena = Arena::new();
        let mut alp_arena = Arena::new();
        let root = self.optimize_with_scratch(&mut alp_arena, &mut expr_arena);
        let alp = alp_arena.take(root);
        alp.to_lp(&mut alp_arena, &mut expr_arena)
    }

    pub fn collect(self) -> DataFrame {
        let mut executor = self.prepare_collect();
        executor.execute()
    }

    fn prepare_collect(self) -> Box<dyn Executor> {
        let mut expr_arena = Arena::new();
        let mut alp_arena = Arena::new();
        let root = self.optimize_with_scratch(&mut alp_arena, &mut expr_arena);
        alp_node_to_physical_plan(root, &mut expr_arena, &mut alp_arena)
    }
}

pub fn optimize(
    logical_plan: LogicalPlan,
    alp_arena: &mut Arena<ALogicalPlan>,
    expr_arena: &mut Arena<AExpr>,
) -> Node {
    todo!()
}
