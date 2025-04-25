use crate::datasrv::DataObjectId;
use crate::internal::common::stablemap::ExtractKey;
use crate::internal::messages::worker::ToWorkerMessage;
use crate::internal::server::comm::Comm;
use crate::{Map, WorkerId};
use smallvec::SmallVec;
use std::collections::HashSet;

pub(crate) type RefCount = u32;

#[derive(Debug)]
pub struct DataObjectHandle {
    // TODO: Optimize for small number of placements (enum?)
    id: DataObjectId,
    placement: HashSet<WorkerId>,
    size: u64,
    ref_count: RefCount,
}

impl DataObjectHandle {
    pub fn new(
        id: DataObjectId,
        initial_placement: WorkerId,
        size: u64,
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

    pub fn size(&self) -> u64 {
        self.size
    }

    #[must_use]
    #[inline]
    pub fn decrease_ref_count(&mut self) -> bool {
        self.ref_count -= 1;
        self.ref_count == 0
    }

    pub fn ref_count(&self) -> RefCount {
        self.ref_count
    }

    pub fn add_placement(&mut self, worker_id: WorkerId) {
        self.placement.insert(worker_id);
    }

    pub fn remove_placement(&mut self, worker_id: WorkerId) -> bool {
        self.placement.remove(&worker_id)
    }

    pub fn placement(&self) -> &HashSet<WorkerId> {
        &self.placement
    }
}

impl ExtractKey<DataObjectId> for DataObjectHandle {
    #[inline]
    fn extract_key(&self) -> DataObjectId {
        self.id
    }
}

pub struct ObjsToRemoveFromWorkers(Map<WorkerId, SmallVec<[DataObjectId; 1]>>);

impl Default for ObjsToRemoveFromWorkers {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjsToRemoveFromWorkers {
    pub fn new() -> Self {
        ObjsToRemoveFromWorkers(Map::new())
    }

    pub fn add(&mut self, worker_id: WorkerId, data_object_id: DataObjectId) {
        self.0.entry(worker_id).or_default().push(data_object_id);
    }

    pub fn send(self, comm: &mut impl Comm) {
        for (worker_id, obj_ids) in self.0 {
            comm.send_worker_message(worker_id, &ToWorkerMessage::RemoveDataObjects(obj_ids));
        }
    }
}
