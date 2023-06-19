use super::expr::Expr;

#[derive(Clone, PartialEq, Debug)]
pub enum LiteralValue {
    Boolean(bool),
    Int32(i32),
    Utf8(String),
}

pub trait Literal {
    fn lit(self) -> Expr;
}

impl Literal for bool {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Boolean(self))
    }
}

impl Literal for i32 {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Int32(self))
    }
}

impl Literal for &str {
    fn lit(self) -> Expr {
        Expr::Literal(LiteralValue::Utf8(self.to_owned()))
    }
}

pub fn lit<L: Literal>(t: L) -> Expr {
    t.lit()
}
