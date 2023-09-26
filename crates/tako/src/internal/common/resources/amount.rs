use derive_more::{Add, AddAssign, Sub, SubAssign, Sum};
use serde::{Deserialize, Serialize};

pub type ResourceUnits = u32;
pub type ResourceFractions = u32;

pub const FRACTIONS_PER_UNIT: ResourceFractions = 10_000;
pub const FRACTIONS_MAX_DIGITS: usize = 4; // = log10(FRACTIONS_PER_UNIT)

#[derive(
    Debug,
    Serialize,
    Clone,
    Copy,
    Hash,
    Eq,
    Deserialize,
    PartialEq,
    Ord,
    PartialOrd,
    AddAssign,
    SubAssign,
    Sub,
    Add,
    Sum,
)]
pub struct ResourceAmount(u64);

impl ResourceAmount {
    pub const ZERO: ResourceAmount = ResourceAmount(0);

    pub fn new(units: ResourceUnits, fractions: ResourceFractions) -> Self {
        assert!(fractions < FRACTIONS_PER_UNIT);
        ResourceAmount(units as u64 * FRACTIONS_PER_UNIT as u64 + fractions as u64)
    }

    pub fn new_units(units: ResourceUnits) -> Self {
        ResourceAmount(units as u64 * FRACTIONS_PER_UNIT as u64)
    }

    pub fn new_fractions(fractions: ResourceFractions) -> Self {
        assert!(fractions < FRACTIONS_PER_UNIT);
        ResourceAmount(fractions as u64)
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }

    pub fn units(&self) -> ResourceUnits {
        (self.0 / (FRACTIONS_PER_UNIT as u64)) as ResourceUnits
    }

    pub fn fractions(&self) -> ResourceFractions {
        (self.0 % (FRACTIONS_PER_UNIT as u64)) as ResourceFractions
    }

    pub fn split(&self) -> (ResourceUnits, ResourceFractions) {
        (self.units(), self.fractions())
    }

    pub fn total_fractions(&self) -> u64 {
        self.0
    }

    pub fn as_f32(&self) -> f32 {
        self.0 as f32 / FRACTIONS_PER_UNIT as f32
    }
}

impl std::fmt::Display for ResourceAmount {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let fractions = self.fractions();
        write!(f, "{}", self.units())?;
        if fractions != 0 {
            let num = format!("{:01$}", fractions, 4);
            write!(f, ".{}", num.trim_end_matches("0"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl ResourceAmount {
        pub fn assert_eq_units(&self, units: ResourceUnits) {
            assert_eq!(self.units(), units);
            assert_eq!(self.fractions(), 0);
        }
    }

    #[test]
    pub fn test_amount_add() {
        let r1 = ResourceAmount::new(10, 1234);
        let r2 = ResourceAmount::new(2, 4321);
        let r3 = ResourceAmount::new(7, 9999);
        assert_eq!(r1 + r2, ResourceAmount::new(12, 5555));
        assert_eq!(r1 + r3, ResourceAmount::new(18, 1233));
        assert_eq!(r1 + ResourceAmount::ZERO, r1);
    }

    #[test]
    pub fn test_amount_display() {
        assert_eq!(ResourceAmount::new(0, 0).to_string(), "0");
        assert_eq!(ResourceAmount::new(0, 1).to_string(), "0.0001");
        assert_eq!(ResourceAmount::new(500, 0).to_string(), "500");
        assert_eq!(ResourceAmount::new(500, 123).to_string(), "500.0123");
        assert_eq!(ResourceAmount::new(500, 9999).to_string(), "500.9999");
        assert_eq!(ResourceAmount::new(1, 1000).to_string(), "1.1");
        assert_eq!(ResourceAmount::new(1, 2200).to_string(), "1.22");
        assert_eq!(ResourceAmount::new(1, 3410).to_string(), "1.341");
    }
}
