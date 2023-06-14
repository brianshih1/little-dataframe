use std::sync::Arc;

use super::{
    arena::{Arena, Node},
    expr::{Expr, Operator},
};

// Original Polars AExpr: https://github.com/pola-rs/polars/blob/f566963f526a11585805088c96e579045a0a2b79/polars/polars-lazy/polars-plan/src/logical_plan/aexpr/mod.rs#L44
#[derive(Clone, Debug)]
pub enum AExpr {
    BinaryExpr {
        left: Node,
        op: Operator,
        right: Node,
    },
    Filter {
        input: Node,
        by: Node,
    },
    Column(Arc<str>),
}

pub fn expr_to_aexpr(expr: Expr, arena: &mut Arena<AExpr>) -> Node {
    let aexpr = match expr {
        Expr::Column(str) => AExpr::Column(str),
        Expr::BinaryExpr { left, op, right } => AExpr::BinaryExpr {
            left: expr_to_aexpr(*left, arena),
            op,
            right: expr_to_aexpr(*right, arena),
        },
        Expr::Filter { input, by } => AExpr::Filter {
            input: expr_to_aexpr(*input, arena),
            by: expr_to_aexpr(*by, arena),
        },
    };
    arena.add(aexpr)
}
