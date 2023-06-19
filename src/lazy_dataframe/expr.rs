use std::{fmt::Debug, sync::Arc};

use super::lit::LiteralValue;

#[derive(Clone)]
pub enum Expr {
    Column(Arc<str>),
    BinaryExpr {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    Literal(LiteralValue),
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Operator {
    And,
    Or,
    Eq,
}

impl Expr {
    pub fn eq(self, other: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self),
            op: Operator::Eq,
            right: Box::new(other),
        }
    }
}

pub fn col(str: &str) -> Expr {
    Expr::Column(Arc::from(str))
}

impl Debug for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(name) => write!(f, "col(\"{name}\")"),
            Expr::BinaryExpr { left, op, right } => write!(f, "[({left:?}) {op:?} ({right:?})]"),
            Expr::Literal(lit) => write!(f, "lit(\"{lit:?}\")"),
        }
    }
}
