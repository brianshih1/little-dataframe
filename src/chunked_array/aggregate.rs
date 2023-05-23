use arrow2::compute::aggregate::max_primitive;

use super::types::I32Chunked;

impl I32Chunked {
    pub fn max(&self) -> Option<i32> {
        self.iter_primitive()
            .filter_map(max_primitive)
            .fold(None, |acc, item| match acc {
                Some(max) => {
                    if max > item {
                        Some(max)
                    } else {
                        Some(item)
                    }
                }
                None => Some(item),
            })
    }
}
