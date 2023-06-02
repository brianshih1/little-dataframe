use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::POOL,
    series::{constructor::IntoSeries, Series},
};

pub mod join;
pub mod utils;

pub struct DataFrame {
    pub columns: Vec<Series>,
}

impl DataFrame {
    pub fn new<S: IntoSeries>(columns: Vec<S>) -> Self {
        if columns.len() == 0 {
            panic!("Dataframe cannot be empty")
        }
        let mut first_len = None;
        let mut arr = Vec::with_capacity(columns.len());
        for s in columns {
            let series = s.into_series();
            match first_len {
                Some(len) => {
                    if len != series.len() {
                        panic!("Series must be the same len")
                    }
                }
                None => first_len = Some(series.len()),
            }
            arr.push(series);
        }

        // TODO: Make sure dataframe doesn't have duplicate names
        DataFrame { columns: arr }
    }

    pub fn new_no_checks(columns: Vec<Series>) -> Self {
        DataFrame { columns }
    }

    pub fn rows_count(&self) -> usize {
        self.columns[0].len()
    }

    pub fn select_series<I, S>(&self, selection: I) -> Vec<Series>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        selection
            .into_iter()
            .map(|s| self.column(s.as_ref()))
            .collect()
    }

    pub fn column(&self, name: &str) -> Series {
        let idx = self.get_index_with_name(name);
        self.get(idx)
    }

    pub fn get(&self, idx: usize) -> Series {
        self.columns[idx].clone()
    }

    pub fn get_index_with_name(&self, name: &str) -> usize {
        self.columns.iter().position(|c| c.name() == name).unwrap()
    }

    // For now, makes all the chunks
    pub fn align_chunks(&mut self) {
        self.columns = self.apply_columns_par(&|series| series.rechunk());
    }

    pub fn apply_columns_par(
        &self,
        func: &(dyn Fn(&Series) -> Series + Sync + Send),
    ) -> Vec<Series> {
        POOL.install(|| self.columns.par_iter().map(|s| func(s)).collect())
    }

    /**
     * If there's df of:
     *  A: [a0, a1, a2, a3]
     *  B: [b0, b1, b2, b3]
     * df.slice(1, 2) gives
     *  A: [a1, a2]
     *  B: [b1, b2]
     */
    pub fn slice(&self, offset: usize, length: usize) -> DataFrame {
        let columns = self
            .columns
            .iter()
            .map(|s| s.slice(offset, length))
            .collect();
        DataFrame::new_no_checks(columns)
    }
}