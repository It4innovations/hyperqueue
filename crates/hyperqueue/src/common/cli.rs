use crate::common::arraydef::IntArray;
use crate::transfer::messages::Selector;
use std::str::FromStr;

pub enum SelectorArg {
    All,
    Last,
    Id(IntArray),
}

impl FromStr for SelectorArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "last" => Ok(SelectorArg::Last),
            "all" => Ok(SelectorArg::All),
            _ => Ok(SelectorArg::Id(IntArray::from_str(s)?)),
        }
    }
}

impl From<SelectorArg> for Selector {
    fn from(selector_arg: SelectorArg) -> Self {
        match selector_arg {
            SelectorArg::Id(array) => Selector::Specific(array),
            SelectorArg::Last => Selector::LastN(1),
            SelectorArg::All => Selector::All,
        }
    }
}
