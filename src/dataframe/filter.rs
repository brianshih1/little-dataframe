use crate::{chunked_array::types::BooleanChunked, series::Series};

use super::DataFrame;

impl DataFrame {
    pub fn filter(&self, mask: &BooleanChunked) -> DataFrame {
        let columns = self.apply_columns_par(&|series| series.filter(mask));
        DataFrame::new(columns)
    }
}
