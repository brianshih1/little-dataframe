use std::sync::Arc;

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
