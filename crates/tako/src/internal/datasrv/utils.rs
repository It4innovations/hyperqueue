use crate::internal::datasrv::DataObject;
use crate::internal::datasrv::messages::DataObjectSlice;
use std::rc::Rc;

pub(crate) const UPLOAD_CHUNK_SIZE: usize = 32 * 1024 * 1024; // 32MiB

pub(crate) struct DataObjectDecomposer {
    data_obj: Rc<DataObject>,
    end: usize,
}

impl DataObjectDecomposer {
    pub fn new(data_obj: Rc<DataObject>) -> (Self, DataObjectSlice) {
        let size = data_obj.size() as usize;
        let end = size.min(UPLOAD_CHUNK_SIZE);
        (
            DataObjectDecomposer {
                data_obj: data_obj.clone(),
                end,
            },
            DataObjectSlice {
                data_object: data_obj,
                start: 0,
                end,
            },
        )
    }
    pub fn next(&mut self) -> Option<DataObjectSlice> {
        let size = self.data_obj.size() as usize;
        if self.end < size {
            let start = self.end;
            self.end = (self.end + UPLOAD_CHUNK_SIZE).min(size);
            Some(DataObjectSlice {
                data_object: self.data_obj.clone(),
                start,
                end: self.end,
            })
        } else {
            None
        }
    }
}

pub(crate) struct DataObjectComposer {
    size: usize,
    data: Vec<u8>,
}

impl DataObjectComposer {
    pub fn new(size: usize, mut data: Vec<u8>) -> Self {
        if data.len() != size {
            data.reserve(size);
        }
        DataObjectComposer { size, data }
    }

    pub fn add(&mut self, mut data: Vec<u8>) -> usize {
        assert!(data.len() + self.data.len() <= self.size);
        self.data.append(&mut data);
        self.data.len()
    }

    pub fn is_finished(&self) -> bool {
        self.size == self.data.len()
    }

    pub fn finish(self, mime_type: String) -> DataObject {
        DataObject::new(mime_type, self.data)
    }
}
