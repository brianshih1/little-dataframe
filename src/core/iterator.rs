use crate::lazy_dataframe::{
    aexpr::AExpr,
    arena::{Arena, Node},
};

pub struct AExprIter<'a> {
    stack: Vec<Node>,
    arena: &'a Arena<AExpr>,
}

impl<'a> AExprIter<'a> {
    pub fn new(stack: Vec<Node>, arena: &'a Arena<AExpr>) -> Self {
        Self { stack, arena }
    }
}

impl<'a> Iterator for AExprIter<'a> {
    type Item = (Node, &'a AExpr);

    fn next(&mut self) -> Option<Self::Item> {
        self.stack.pop().map(|node| {
            let aexpr = self.arena.get(node);
            aexpr.add_nodes_to_stack(&mut self.stack);
            (node, aexpr)
        })
    }
}
