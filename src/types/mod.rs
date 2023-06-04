use arrow2::datatypes::DataType as ArrowDataType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Int32,
    Utf8,
    Boolean,
}

pub trait LittleDataType {
    fn get_dtype() -> DataType;
}

impl DataType {
    pub fn to_array_type(&self) -> ArrowDataType {
        match self {
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Utf8 => ArrowDataType::LargeUtf8,
            DataType::Boolean => ArrowDataType::Boolean,
        }
    }
}
