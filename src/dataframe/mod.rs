use crate::series::{constructor::IntoSeries, Series};

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

    pub fn select_series(&self, selection: &Vec<String>) -> Vec<Series> {
        selection.iter().map(|s| self.column(s)).collect()
    }

    pub fn column(&self, name: &str) -> Series {
        let idx = self.get_index_with_name(name);
        self.get(idx)
    }

    pub fn get(&self, idx: usize) -> Series {
        self.columns[idx]
    }

    pub fn get_index_with_name(&self, name: &str) -> usize {
        self.columns.iter().position(|c| c.name() == name).unwrap()
    }
}
