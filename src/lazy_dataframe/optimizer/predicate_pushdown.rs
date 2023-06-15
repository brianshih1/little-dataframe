use crate::lazy_dataframe::{
    alogical_plan::ALogicalPlan,
    arena::Arena,
    logical_plan::{self, LogicalPlan},
};

struct PredicatePushdown {}

impl PredicatePushdown {
    pub fn optimize(
        &self,
        logical_plan: LogicalPlan,
        alp_arena: &mut Arena<ALogicalPlan>,
        expr_arena: &mut Arena<ALogicalPlan>,
    ) -> ALogicalPlan {
        todo!()
    }
}
