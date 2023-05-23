use super::types::{BooleanChunked, I32Chunked, Utf8Chunked};

pub trait AssertUtils<TItem> {
    fn equals_lists(&self, items: &[&[TItem]]) -> ();
}

impl AssertUtils<bool> for BooleanChunked {
    fn equals_lists(&self, items: &[&[bool]]) -> () {
        assert_eq!(self.chunks.len(), items.len());

        self.iter_primitive()
            .zip(items.iter())
            .for_each(|(chunk, items)| {
                assert_eq!(chunk.len(), items.len());
                chunk
                    .iter()
                    .zip(items.iter())
                    .for_each(|(chunk_item, item)| {
                        assert_eq!(&chunk_item.unwrap(), item);
                    })
            });
    }
}

impl AssertUtils<i32> for I32Chunked {
    fn equals_lists(&self, items: &[&[i32]]) -> () {
        assert_eq!(self.chunks.len(), items.len());

        self.iter_primitive()
            .zip(items.iter())
            .for_each(|(chunk, items)| {
                assert_eq!(chunk.len(), items.len());
                chunk
                    .iter()
                    .zip(items.iter())
                    .for_each(|(chunk_item, item)| {
                        assert_eq!(chunk_item.unwrap(), item);
                    })
            });
    }
}

impl AssertUtils<&str> for Utf8Chunked {
    fn equals_lists(&self, items: &[&[&str]]) -> () {
        assert_eq!(self.chunks.len(), items.len());

        self.iter_primitive()
            .zip(items.iter())
            .for_each(|(chunk, items)| {
                assert_eq!(chunk.len(), items.len());
                chunk
                    .iter()
                    .zip(items.iter())
                    .for_each(|(chunk_item, item)| {
                        assert_eq!(&chunk_item.unwrap(), item);
                    })
            });
    }
}
