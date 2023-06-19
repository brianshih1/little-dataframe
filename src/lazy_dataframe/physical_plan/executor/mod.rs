use crate::dataframe::DataFrame;

pub mod data_frame_scan;
pub mod filter;
mod filter_test;
pub mod join;

// TODO: Why is PhysicalExpr Sync + Send but Executor is just Send...?
pub trait Executor: Send {
    fn execute(&mut self) -> DataFrame;
}
