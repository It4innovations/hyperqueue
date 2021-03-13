use serde::{Deserialize, Serialize, Serializer};

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub enum SerializationType {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "pickle")]
    Pickle,
}

/*
   Default serialization of a simple enum is
   done in a way that cannot be deserialized in python.
*/
impl Serialize for SerializationType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(match self {
            Self::None => "none",
            Self::Pickle => "pickle",
        })
    }
}
