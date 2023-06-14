use std::sync::Arc;

use crate::dataframe::{join::JoinType, DataFrame};

use super::{
    aexpr::{expr_to_aexpr, AExpr},
    arena::{Arena, Node},
    logical_plan::LogicalPlan,
};

// Original ALogicalPlan from Polars:
// https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-lazy/polars-plan/src/logical_plan/alp.rs#L22
#[derive(Clone, Debug)]
pub enum ALogicalPlan {
    Join {
        left: Node,
        right: Node,
        left_on: Vec<Node>,
        right_on: Vec<Node>,
        join_type: JoinType,
    },
    Selection {
        input: Node,
        predicate: Node,
    },
    DataFrameScan {
        df: Arc<DataFrame>,
        projection: Option<Arc<Vec<String>>>,
        selection: Option<Node>,
    },
}

pub fn logical_to_alp(
    lp: LogicalPlan,
    expr_arena: &mut Arena<AExpr>,
    alp_arena: &mut Arena<ALogicalPlan>,
) -> Node {
    let node = match lp {
        LogicalPlan::Join {
            left,
            right,
            left_on,
            right_on,
            join_type,
        } => ALogicalPlan::Join {
            left: logical_to_alp(*left, expr_arena, alp_arena),
            right: logical_to_alp(*right, expr_arena, alp_arena),
            left_on: left_on
                .into_iter()
                .map(|expr| expr_to_aexpr(expr, expr_arena))
                .collect(),
            right_on: right_on
                .into_iter()
                .map(|expr| expr_to_aexpr(expr, expr_arena))
                .collect(),
            join_type,
        },
        LogicalPlan::Selection { input, predicate } => ALogicalPlan::Selection {
            input: logical_to_alp(*input, expr_arena, alp_arena),
            predicate: expr_to_aexpr(predicate, expr_arena),
        },
        LogicalPlan::DataFrameScan {
            df,
            projection,
            selection,
        } => ALogicalPlan::DataFrameScan {
            df,
            projection,
            selection: selection.map(|expr| expr_to_aexpr(expr, expr_arena)),
        },
    };
    alp_arena.add(node)
}
