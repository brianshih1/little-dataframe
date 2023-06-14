#[derive(Clone)]
pub struct Arena<T> {
    items: Vec<T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Node(pub usize);

impl<T> Arena<T> {
    pub fn add(&mut self, value: T) -> Node {
        let len = self.items.len();
        self.items.push(value);
        Node(len)
    }
}
