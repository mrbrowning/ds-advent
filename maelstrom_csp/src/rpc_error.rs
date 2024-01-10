use core::fmt;
use std::io;

use serde::{Deserialize, Serialize};
use thiserror;

use crate::{message::ErrorCode, types::JSONMap};

const ERROR_MSG_TYPE: &str = "error";

#[derive(thiserror::Error, Debug)]
pub enum MaelstromError {
    #[error("RPC error: {0}")]
    RPCError(#[from] RPCError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IOError(#[from] io::Error),

    #[error("Channel error: {0}")]
    ChannelError(String),

    #[error("Error: {0}")]
    Other(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ErrorType {
    Timeout = 0,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

impl From<ErrorType> for ErrorCode {
    fn from(value: ErrorType) -> Self {
        (value as i64).into()
    }
}

impl TryFrom<ErrorCode> for ErrorType {
    type Error = MaelstromError;

    fn try_from(value: ErrorCode) -> Result<Self, Self::Error> {
        match value.into() {
            0 => Ok(Self::Timeout),
            10 => Ok(Self::NotSupported),
            11 => Ok(Self::TemporarilyUnavailable),
            12 => Ok(Self::MalformedRequest),
            13 => Ok(Self::Crash),
            14 => Ok(Self::Abort),
            20 => Ok(Self::KeyDoesNotExist),
            21 => Ok(Self::KeyAlreadyExists),
            22 => Ok(Self::PreconditionFailed),
            30 => Ok(Self::TxnConflict),
            _ => Err(MaelstromError::Other(format!(
                "Can't cast {} to ErrorType, no matching code",
                value
            ))),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub struct RPCError {
    code: ErrorCode,
    text: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct RPCErrorJSON {
    #[serde(rename = "type")]
    msg_type: String,
    code: ErrorCode,
    text: String,
}

impl RPCError {
    pub fn new(code: ErrorCode, text: impl Into<String>) -> Self {
        RPCError {
            code,
            text: text.into(),
        }
    }

    pub fn error_code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.text
    }

    pub fn error_code_text(code: ErrorCode) -> String {
        if let Ok(err_type) = ErrorType::try_from(code) {
            return format!("{:?}", err_type);
        }

        format!("ErrorCode<{}>", code)
    }
}

impl fmt::Display for RPCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RPCError({}, \"{}\")",
            RPCError::error_code_text(self.code),
            self.text
        )
    }
}

impl Serialize for RPCError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let rpc_err_json = RPCErrorJSON {
            msg_type: ERROR_MSG_TYPE.to_string(),
            code: self.code,
            text: self.text.clone(),
        };
        rpc_err_json.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RPCError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let rpc_err_json = RPCErrorJSON::deserialize(deserializer)?;

        Ok(RPCError {
            code: rpc_err_json.code,
            text: rpc_err_json.text,
        })
    }
}

impl From<RPCError> for JSONMap {
    fn from(value: RPCError) -> Self {
        let mut map = JSONMap::new();
        map.insert("type".to_string(), serde_json::Value::from(ERROR_MSG_TYPE));
        map.insert(
            "code".to_string(),
            serde_json::Value::from(i64::from(value.code)),
        );
        map.insert("text".to_string(), serde_json::Value::from(value.text));

        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_text() {
        let test_vals: Vec<(ErrorCode, &str)> = vec![
            (ErrorType::Timeout.into(), "Timeout"),
            (ErrorType::NotSupported.into(), "NotSupported"),
            (
                ErrorType::TemporarilyUnavailable.into(),
                "TemporarilyUnavailable",
            ),
            (ErrorType::MalformedRequest.into(), "MalformedRequest"),
            (ErrorType::Crash.into(), "Crash"),
            (ErrorType::Abort.into(), "Abort"),
            (ErrorType::KeyDoesNotExist.into(), "KeyDoesNotExist"),
            (ErrorType::KeyAlreadyExists.into(), "KeyAlreadyExists"),
            (ErrorType::PreconditionFailed.into(), "PreconditionFailed"),
            (ErrorType::TxnConflict.into(), "TxnConflict"),
            (1000.into(), "ErrorCode<1000>"),
        ];

        for (code, code_str) in test_vals {
            assert_eq!(code_str, RPCError::error_code_text(code));
        }
    }

    #[test]
    fn test_rpc_error_display() {
        assert_eq!(
            format!("{}", RPCError::new(ErrorType::Crash.into(), "foo")),
            "RPCError(Crash, \"foo\")"
        );
    }
}
