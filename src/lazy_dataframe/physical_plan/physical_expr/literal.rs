use crate::{
    chunked_array::{
        chunk_full::ChunkFull,
        types::{BooleanChunked, I32Chunked, Utf8Chunked},
    },
    dataframe::DataFrame,
    lazy_dataframe::lit::LiteralValue,
    series::{constructor::IntoSeries, Series},
};

use super::PhysicalExpr;

pub struct LiteralExpr(pub LiteralValue);

impl LiteralExpr {
    pub fn new(lit: LiteralValue) -> Self {
        LiteralExpr(lit)
    }
}

impl PhysicalExpr for LiteralExpr {
    fn evaluate(&self, df: &DataFrame) -> Series {
        let rows_count = df.rows_count();
        let series_name = "LITERAL";
        // TODO: We should create a Series of size 1. But
        // currently our algorithms doesn't work when Series size
        // is 1.
        match &self.0 {
            LiteralValue::Boolean(v) => {
                BooleanChunked::full(series_name, *v, rows_count).into_series()
            }
            LiteralValue::Int32(v) => I32Chunked::full(series_name, *v, rows_count).into_series(),
            LiteralValue::Utf8(v) => Utf8Chunked::full(series_name, v, rows_count).into_series(),
        }
    }
}
