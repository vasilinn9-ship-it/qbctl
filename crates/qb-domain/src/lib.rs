use std::{error::Error, fmt, str::FromStr};

const MAX_ID_LEN: usize = 128;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IdError;

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "identifier must be non-empty, at most 128 bytes, and contain no control characters",
        )
    }
}

impl Error for IdError {}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_LEN && !value.chars().any(char::is_control)
}

macro_rules! strong_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if valid_id(&value) {
                    Ok(Self(value))
                } else {
                    Err(IdError)
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }
    };
}

strong_id!(RequestId);
strong_id!(OperationId);
strong_id!(JobId);
strong_id!(PlanId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PolicyRevision(u64);

impl PolicyRevision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_reject_empty_and_control_characters() {
        assert!(RequestId::new("").is_err());
        assert!(RequestId::new("bad\nvalue").is_err());
        assert!(RequestId::new("agent-123").is_ok());
    }
}
