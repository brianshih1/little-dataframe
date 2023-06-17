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

    pub fn get_mut(&mut self, idx: Node) -> &mut T {
        self.items.get_mut(idx.0).unwrap()
    }

    pub fn get(&self, idx: Node) -> &T {
        self.items.get(idx.0).unwrap()
    }
}

impl<T: Default> Arena<T> {
    pub fn take(&mut self, idx: Node) -> T {
        std::mem::take(self.get_mut(idx))
    }
}
