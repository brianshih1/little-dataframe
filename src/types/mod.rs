use arrow2::datatypes::DataType as ArrowDataType;

pub enum DataType {
    Int32,
    Utf8,
}

impl DataType {
    pub fn to_array_type(&self) -> ArrowDataType {
        match self {
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Utf8 => ArrowDataType::LargeUtf8,
        }
    }
}
