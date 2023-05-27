use crate::chunked_array::types::{BooleanChunked, I32Chunked, Utf8Chunked};

use super::{series_trait::SeriesTrait, SeriesWrap};

impl SeriesTrait for SeriesWrap<BooleanChunked> {}

impl SeriesTrait for SeriesWrap<I32Chunked> {}

impl SeriesTrait for SeriesWrap<Utf8Chunked> {}
