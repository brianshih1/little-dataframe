use std::collections::hash_map::RandomState;

use crate::types::{DataType, LittleDataType};

pub trait SeriesTrait {
    fn dtype(&self) -> DataType;

    fn len(&self) -> usize;

    fn name(&self) -> &str;

    fn vec_hash(&self, _hasher: RandomState, buf: &mut Vec<u64>);

    fn vec_hash_combine(&self, _hasher: RandomState, buf: &mut Vec<u64>);
}
