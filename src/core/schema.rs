use std::sync::Arc;

use indexmap::{map::Iter, IndexMap};

use crate::types::DataType;

use super::field::Field;

#[derive(Clone, Debug)]
pub struct Schema {
    columns: IndexMap<String, DataType>,
}

pub type SchemaRef = Arc<Schema>;

impl Schema {
    pub fn new() -> Self {
        Schema {
            columns: IndexMap::new(),
        }
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.get_index_of(name)
    }

    pub fn iter(&self) -> Iter<String, DataType> {
        self.columns.iter()
    }

    pub fn with_column(&mut self, name: String, dtype: DataType) {
        self.columns.insert(name, dtype);
    }

    pub fn get_field(&self, name: &str) -> Option<Field> {
        self.columns.get(name).map(|dtype| Field {
            name: name.into(),
            dtype: dtype.clone(),
        })
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
