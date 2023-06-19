use indexmap::IndexMap;

use crate::types::DataType;

use super::field::Field;

#[derive(Clone, Debug)]
pub struct Schema {
    columns: IndexMap<String, DataType>,
}

impl Schema {
    fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.get_index_of(name)
    }
}

impl<F> FromIterator<F> for Schema
where
    F: Into<Field>,
{
    fn from_iter<T: IntoIterator<Item = F>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut hashmap = IndexMap::with_capacity(iter.size_hint().0);
        iter.for_each(|ele| {
            let field: Field = ele.into();
            hashmap.insert(field.name, field.dtype);
        });
        Schema { columns: hashmap }
    }
}
