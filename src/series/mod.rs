use std::{ops::Deref, sync::Arc};

use self::series_trait::SeriesTrait;

pub mod constructor;
pub mod constructor_test;
pub mod implementations;
pub mod series_trait;

#[derive(Clone)]
pub struct Series(pub Arc<dyn SeriesTrait>);

pub struct SeriesWrap<T>(T);

impl Deref for Series {
    type Target = dyn SeriesTrait;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
