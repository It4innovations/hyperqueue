use crate::internal::common::resources::{ResourceId, ResourceVec};
use crate::resources::ResourceAmount;
use smallvec::{smallvec, SmallVec};

/// Resource counts of a one resource_id
/// In most cases len() == 1
/// len() > 1 only if resource is group resource
pub type ResourceCount = SmallVec<[ResourceAmount; 1]>;

/// Resource counts per resource_id
#[derive(Default, PartialEq, Eq, Debug, Clone)]
pub struct ResourceCountVec {
    counts: ResourceVec<ResourceCount>,
}

pub fn resource_count_add_at(count: &mut ResourceCount, group_idx: usize, value: ResourceAmount) {
    if group_idx >= count.len() {
        count.resize(group_idx + 1, 0);
    }
    count[group_idx] += value;
}

fn resource_count_add(first: &mut ResourceCount, other: &ResourceCount) {
    if first.len() < other.len() {
        first.resize(other.len(), 0);
    }
    for (t, v) in first.iter_mut().zip(other.iter()) {
        *t += v;
    }
}

fn resource_count_remove(first: &mut ResourceCount, other: &ResourceCount) -> bool {
    if first.len() < other.len() {
        first.resize(other.len(), 0);
    }
    let mut valid = true;
    for (t, v) in first.iter_mut().zip(other.iter()) {
        if *t < *v {
            *t = 0;
            valid = false;
        }
        *t -= v;
    }
    valid
}

impl ResourceCountVec {
    pub fn new(counts: ResourceVec<ResourceCount>) -> Self {
        ResourceCountVec { counts }
    }

    pub fn set(&mut self, resource_id: ResourceId, count: ResourceCount) {
        if resource_id.as_num() as usize >= self.counts.len() {
            self.counts
                .resize(resource_id.as_num() as usize + 1, smallvec![])
        }
        self.counts[resource_id] = count;
    }

    pub fn get_mut(&mut self, resource_id: ResourceId) -> &mut ResourceCount {
        &mut self.counts[resource_id]
    }

    pub fn get(&self, resource_id: ResourceId) -> &ResourceCount {
        &self.counts[resource_id]
    }

    pub fn len(&self) -> usize {
        self.counts.len()
    }

    fn match_sizes(&mut self, other: &ResourceCountVec) {
        if self.counts.len() < other.counts.len() {
            self.counts.resize(other.counts.len(), smallvec![]);
        }
    }

    pub fn add(&mut self, other: &ResourceCountVec) {
        self.match_sizes(other);
        for (t, v) in self.counts.iter_mut().zip(other.counts.iter()) {
            resource_count_add(t, v);
        }
    }

    pub fn remove(&mut self, other: &ResourceCountVec) -> bool {
        self.match_sizes(other);
        let mut valid = true;
        for (t, v) in self.counts.iter_mut().zip(other.counts.iter()) {
            valid &= resource_count_remove(t, v);
        }
        valid
    }

    pub fn fraction(&self, bounds: &ResourceCountVec) -> f32 {
        self.counts
            .iter()
            .zip(bounds.counts.iter())
            .map(|(x, y)| {
                let sy: ResourceAmount = y.iter().sum();
                if sy == 0 {
                    0f32
                } else {
                    let sx: ResourceAmount = x.iter().sum();
                    sx as f32 / sy as f32
                }
            })
            .sum()
    }

    pub fn all_counts(&self) -> &[ResourceCount] {
        &self.counts
    }
    /*pub fn from_request(request: ResourceRequest) -> Self {
        ResourceCounter { counts: request }
    }*/
}

#[cfg(test)]
mod tests {
    use crate::internal::worker::counts::ResourceCountVec;
    use crate::resources::ResourceAmount;
    use smallvec::smallvec;

    impl ResourceCountVec {
        pub fn new_simple(counts: &[ResourceAmount]) -> Self {
            let counts: Vec<_> = counts.into_iter().map(|x| smallvec![*x]).collect();
            Self::new(counts.into())
        }

        pub fn assert_eq(&self, counts: &[ResourceAmount]) {
            assert_eq!(self, &ResourceCountVec::new_simple(counts));
        }
    }

    #[test]
    fn test_counts_add() {
        let counts1 = vec![
            smallvec![],
            smallvec![0, 1],
            smallvec![1, 0],
            smallvec![1024],
        ];
        let mut v1 = ResourceCountVec::new(counts1.into());

        let counts2 = vec![
            smallvec![21],
            smallvec![2, 1],
            smallvec![0, 3],
            smallvec![444],
            smallvec![2],
        ];
        let v2 = ResourceCountVec::new(counts2.into());

        let counts3 = vec![
            smallvec![21],
            smallvec![2, 2],
            smallvec![1, 3],
            smallvec![1468],
            smallvec![2],
        ];
        let v3 = ResourceCountVec::new(counts3.into());

        v1.add(&v2);
        assert_eq!(v1, v3);
    }
}
