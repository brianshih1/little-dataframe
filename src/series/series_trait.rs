use crate::types::{DataType, LittleDataType};

pub trait SeriesTrait {
    fn dtype(&self) -> &DataType;

    fn len(&self) -> usize;
}
