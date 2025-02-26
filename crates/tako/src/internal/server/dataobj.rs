use crate::datasrv::DataObjectId;
use crate::internal::common::stablemap::ExtractKey;
use crate::internal::server::task::Task;
use crate::{TaskId, WorkerId};
use std::collections::HashSet;

pub(crate) type RefCount = u32;

#[derive(Debug)]
pub(crate) struct DataObjectHandle {
    // TODO: Optimize for small number of placements (enum?)
    id: DataObjectId,
    placement: HashSet<WorkerId>,
    size: usize,
    ref_count: RefCount,
}

impl DataObjectHandle {
    pub fn new(
        id: DataObjectId,
        initial_placement: WorkerId,
        size: usize,
        ref_count: RefCount,
    ) -> Self {
        let mut placement = HashSet::new();
        placement.insert(initial_placement);
        DataObjectHandle {
            id,
            placement,
            size,
            ref_count,
        }
    }

    pub fn ref_count(&self) -> RefCount {
        self.ref_count
    }

    pub fn decrease_ref_count(&mut self) -> bool {
        self.ref_count -= 1;
        self.ref_count == 0
    }

    pub fn placement(&self) -> impl Iterator<Item = WorkerId> + '_ {
        self.placement.iter().copied()
    }
}

impl ExtractKey<DataObjectId> for DataObjectHandle {
    #[inline]
    fn extract_key(&self) -> DataObjectId {
        self.id
    }
}
