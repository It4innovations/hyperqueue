use std::str::FromStr;
use std::time::Duration;

pub struct ArgDuration(Duration);

impl ArgDuration {
    pub fn into_duration(self) -> Duration {
        self.0
    }
}

impl FromStr for ArgDuration {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(humantime::parse_duration(s)?))
    }
}
