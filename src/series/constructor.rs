use std::sync::Arc;

use crate::{
    chunked_array::{
        builder::NewFrom,
        types::{BooleanType, I32Chunked, I32Type, Utf8Chunked, Utf8Type},
        ChunkedArray,
    },
    types::LittleDataType,
};

use super::{series_trait::SeriesTrait, Series, SeriesWrap};

macro_rules! impl_new_from {
    ($ty: ty, $little_data_type: ident) => {
        impl NewFrom<$ty> for Series {
            fn new(name: &str, v: &[$ty]) -> Self {
                ChunkedArray::<$little_data_type>::new(name, v).into_series()
            }

            fn from_slice_options(_name: &str, _v: &[Option<$ty>]) -> Self {
                todo!()
            }

            fn from_vec(_name: &str, _v: &[$ty]) -> Self {
                todo!()
            }

            #[cfg(test)]
            fn from_lists(name: &str, _lists: Vec<&[$ty]>) -> Self {
                todo!()
            }
        }
    };
}

impl_new_from!(bool, BooleanType);
impl_new_from!(i32, I32Type);
impl_new_from!(&str, Utf8Type);

pub trait IntoSeries {
    fn into_series(self) -> Series;
}

impl<T: LittleDataType + 'static> IntoSeries for ChunkedArray<T>
where
    SeriesWrap<ChunkedArray<T>>: SeriesTrait,
{
    fn into_series(self) -> Series {
        Series(Arc::new(SeriesWrap(self)))
    }
}

impl IntoSeries for Series {
    fn into_series(self) -> Series {
        self
    }
}
