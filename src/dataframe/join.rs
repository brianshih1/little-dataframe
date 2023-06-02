use std::collections::hash_map::RandomState;

use crate::series::Series;

use super::DataFrame;

impl DataFrame {
    fn inner_join<I, S>(&self, df2: &DataFrame, select_1: I, select_2: I) -> DataFrame
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // TODO: Type checking
        let foo = self.select_series(select_1);
        todo!()
    }

    fn hash_series(series: Vec<Series>) -> Vec<u64> {
        if series.len() == 0 {
            panic!("empty series vec")
        }
        let first = series.first().unwrap();
        let mut buf = Vec::with_capacity(series.len());
        first.vec_hash(RandomState::default(), &mut buf);
        todo!()
    }
}
