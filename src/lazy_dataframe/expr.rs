use std::sync::Arc;

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

pub enum Operator {
    And,
    Or,
}
