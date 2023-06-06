use std::fmt::Debug;

use super::types::{BooleanChunked, I32Chunked, Utf8Chunked};

macro_rules! format_chunked_array {
    ($f:ident, $chunked_array:expr) => {{
        let chunks = &$chunked_array.chunks;
        chunks.iter().enumerate().for_each(|(index, value)| {
            writeln!($f, "Name: {} - {:?}", index, value).unwrap();
        });
        write!($f, "")
    }};
}

impl Debug for BooleanChunked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_chunked_array!(f, &self)
    }
}

impl Debug for I32Chunked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_chunked_array!(f, &self)
    }
}

impl Debug for Utf8Chunked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_chunked_array!(f, &self)
    }
}
