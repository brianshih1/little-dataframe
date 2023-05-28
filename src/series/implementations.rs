use crate::chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked};

use super::{series_trait::SeriesTrait, SeriesWrap};

impl SeriesTrait for SeriesWrap<BooleanChunked> {
    fn dtype(&self) -> &crate::types::DataType {
        todo!()
    }
}

impl SeriesTrait for SeriesWrap<I32Chunked> {
    fn dtype(&self) -> &crate::types::DataType {
        todo!()
    }
}

impl SeriesTrait for SeriesWrap<Utf8Chunked> {
    fn dtype(&self) -> &crate::types::DataType {
        todo!()
    }
}
