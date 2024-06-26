use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum MnMError {
    ApiUnreachable,
}

impl Error for MnMError {}

impl fmt::Display for MnMError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MnMError::ApiUnreachable => write!(f, "API unreachable"),
        }
    }
}
