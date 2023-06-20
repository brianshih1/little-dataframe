use std::sync::Arc;

use hashbrown::{hash_map::Entry, HashMap};

use crate::lazy_dataframe::{
    aexpr::{aexpr_to_leaf_names_iter, AExpr},
    alogical_plan::ALogicalPlan,
    arena::{self, Arena, Node},
    expr::Operator,
    logical_plan::{self, LogicalPlan},
};

pub struct PredicatePushdown {}

impl PredicatePushdown {
    pub fn optimize(
        &self,
        logical_plan: ALogicalPlan,
        alp_arena: &mut Arena<ALogicalPlan>,
        expr_arena: &mut Arena<AExpr>,
    ) -> ALogicalPlan {
        let acc_predicates = HashMap::new();
        self.push_down(logical_plan, alp_arena, expr_arena, acc_predicates)
    }

    fn push_down(
        &self,
        logical_plan: ALogicalPlan,
        alp_arena: &mut Arena<ALogicalPlan>,
        expr_arena: &mut Arena<AExpr>,
        mut acc_predicates: HashMap<Arc<str>, Node>,
    ) -> ALogicalPlan {
        match logical_plan {
            ALogicalPlan::Join {
                left,
                right,
                left_on,
                right_on,
                join_type,
                schema,
            } => {
                let left_schema = alp_arena.get(left).schema(&alp_arena);
                let right_schema = alp_arena.get(right).schema(&alp_arena);
                for (name, predicate_node) in acc_predicates.iter() {}
                todo!()
            }
            ALogicalPlan::Selection { input, predicate } => {
                let local_predicates = extract_local_predicates(&mut acc_predicates, |node| {
                    predicate_is_pushdown_boundary(node, expr_arena)
                });
                insert_and_combine_predicate(&mut acc_predicates, predicate, expr_arena);
                let new_input =
                    self.push_down(alp_arena.take(input), alp_arena, expr_arena, acc_predicates);

                self.optional_wrap_selection(new_input, local_predicates, alp_arena, expr_arena)
            }
            ALogicalPlan::DataFrameScan {
                df,
                projection,
                selection,
                schema,
            } => todo!(),
        }
    }

    fn optional_wrap_selection(
        &self,
        lp: ALogicalPlan,
        local_predicates: Vec<Node>,
        alp_arena: &mut Arena<ALogicalPlan>,
        expr_arena: &mut Arena<AExpr>,
    ) -> ALogicalPlan {
        if local_predicates.is_empty() {
            lp
        } else {
            let predicate = combine_predicates(local_predicates.into_iter(), expr_arena);
            let input = alp_arena.add(lp);
            ALogicalPlan::Selection { input, predicate }
        }
    }
}

pub fn predicate_to_key(predicate: Node, expr_arena: &Arena<AExpr>) -> Arc<str> {
    let names = aexpr_to_leaf_names_iter(predicate, expr_arena).collect::<Vec<Arc<str>>>();
    names.join(":").into()
}

pub(super) fn insert_and_combine_predicate(
    acc_predicates: &mut HashMap<Arc<str>, Node>,
    predicate: Node,
    expr_arena: &mut Arena<AExpr>,
) {
    let predicate_key = predicate_to_key(predicate, expr_arena);
    acc_predicates
        .entry(predicate_key)
        .and_modify(|original_pred| {
            let new_node = AExpr::BinaryExpr {
                left: *original_pred,
                op: Operator::And,
                right: predicate,
            };
            let new_node = expr_arena.add(new_node);
            *original_pred = new_node;
        })
        .or_insert_with(|| predicate);
}

// Returns true if predicate cannot be pushed down
pub fn predicate_is_pushdown_boundary(node: Node, expr_arena: &Arena<AExpr>) -> bool {
    // TODO: Add code here once we have Agg
    // let mut iter = expr_arena.iter(node);
    // iter.all(|(_, expr)| !matches!(expr))
    false
}

pub fn extract_local_predicates<F>(
    acc_predicates: &mut HashMap<Arc<str>, Node>,
    mut condition: F,
) -> Vec<Node>
where
    F: FnMut(Node) -> bool,
{
    let mut remove_keys = Vec::new();
    for (name, node) in &*acc_predicates {
        if condition(*node) {
            remove_keys.push(name.clone());
        }
    }

    let mut local_predicates = Vec::new();
    for key in remove_keys.iter() {
        let predicate = acc_predicates.remove(key);
        if let Some(predicate) = predicate {
            local_predicates.push(predicate);
        }
    }
    local_predicates
}

pub(super) fn combine_predicates<I>(iter: I, exp_arena: &mut Arena<AExpr>) -> Node
where
    I: Iterator<Item = Node>,
{
    let mut curr = None;
    for node in iter {
        match curr {
            Some(prev_node) => {
                curr = Some(exp_arena.add(AExpr::BinaryExpr {
                    left: prev_node,
                    op: Operator::And,
                    right: node,
                }))
            }
            None => curr = Some(node),
        }
    }
    curr.unwrap()
}
