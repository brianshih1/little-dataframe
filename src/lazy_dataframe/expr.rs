use std::sync::Arc;

#[derive(Clone)]
pub enum Expr {
    Column(Arc<str>),
    BinaryExpr {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    Filter {
        input: Box<Expr>,
        by: Box<Expr>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Operator {
    And,
    Or,
}
