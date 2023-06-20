use std::{iter::FilterMap, sync::Arc};

use crate::core::{field::Field, iterator::AExprIter, schema::Schema};

use super::{
    arena::{Arena, Node},
    expr::{Expr, Operator},
    lit::LiteralValue,
    physical_plan::physical_expr::{
        binary_expr::BinaryExpr, column::ColumnExpr, literal::LiteralExpr, PhysicalExpr,
    },
};

// Original Polars AExpr: https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-lazy/polars-plan/src/logical_plan/aexpr/mod.rs#L44
#[derive(Clone, Debug)]
pub enum AExpr {
    BinaryExpr {
        left: Node,
        op: Operator,
        right: Node,
    },
    Column(Arc<str>),
    Literal(LiteralValue),
}

impl AExpr {
    pub fn add_nodes_to_stack(&self, stack: &mut Vec<Node>) {
        match self {
            AExpr::BinaryExpr { left, right, .. } => {
                stack.push(*left);
                stack.push(*right);
            }
            AExpr::Column(_) => {}
            AExpr::Literal(_) => {}
        }
    }

    pub fn to_field(&self, schema: &Schema) -> Field {
        match self {
            AExpr::BinaryExpr { left, op, right } => todo!(),
            AExpr::Column(col_name) => schema.get_field(&col_name).unwrap(),
            AExpr::Literal(_) => todo!(),
        }
    }
}

pub fn expr_to_aexpr(expr: Expr, arena: &mut Arena<AExpr>) -> Node {
    let aexpr = match expr {
        Expr::Column(str) => AExpr::Column(str.clone()),
        Expr::BinaryExpr { left, op, right } => AExpr::BinaryExpr {
            left: expr_to_aexpr(*left, arena),
            op: op.clone(),
            right: expr_to_aexpr(*right, arena),
        },
        Expr::Literal(v) => AExpr::Literal(v),
    };
    arena.add(aexpr)
}

pub fn create_physical_expr(expr: Node, expr_arena: &mut Arena<AExpr>) -> Arc<dyn PhysicalExpr> {
    match expr_arena.get(expr).clone() {
        AExpr::BinaryExpr { left, op, right } => Arc::new(BinaryExpr::new(
            create_physical_expr(left, expr_arena),
            op,
            create_physical_expr(right, expr_arena),
        )),
        AExpr::Column(col_name) => Arc::new(ColumnExpr::new(col_name)),
        AExpr::Literal(lit) => Arc::new(LiteralExpr::new(lit)),
    }
}

fn is_leaf(expr: &AExpr) -> bool {
    matches!(expr, AExpr::Column(_))
}

impl Arena<AExpr> {
    pub fn iter(&self, root: Node) -> AExprIter {
        AExprIter::new(vec![root], self)
    }
}

pub fn aexpr_to_leaf_nodes_iter<'a>(
    root: Node,
    arena: &'a Arena<AExpr>,
) -> FilterMap<AExprIter<'a>, fn((Node, &'a AExpr)) -> Option<Node>> {
    arena
        .iter(root)
        .filter_map(|(node, aexpr)| if is_leaf(aexpr) { Some(node) } else { None })
}

pub fn aexpr_to_leaf_names_iter<'a>(
    node: Node,
    arena: &'a Arena<AExpr>,
) -> impl Iterator<Item = Arc<str>> + 'a {
    aexpr_to_leaf_nodes_iter(node, arena).map(|node| match arena.get(node) {
        AExpr::Column(name) => name.clone(),
        _ => panic!("is not leaf node"),
    })
}

//check if all the leaf nodes are a part of the schema.
pub fn check_input_node(node: Node, schema: &Schema, expr_arena: &Arena<AExpr>) -> bool {
    aexpr_to_leaf_names_iter(node, expr_arena).all(|name| schema.index_of(name.as_ref()).is_some())
}
