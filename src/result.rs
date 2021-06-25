use serde::{Deserialize, Serialize};
use std::{
    error::Error as StdError,
    fmt::{self, Display},
    io,
};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Error {
    Io(String),
    Config(String),
    FileAccess(String),
    WrongObjectType(String),
    DataCorrupted(String),
    ProgramLimitExceed(String),
    InvalidState(String),
    InvalidArgument(String),
    Internal(String),
    OutOfMemory,
    RequestAborted,
}

impl From<io::Error> for Error {
    fn from(ioe: io::Error) -> Self {
        Error::Io(ioe.to_string())
    }
}

impl From<config::ConfigError> for Error {
    fn from(err: config::ConfigError) -> Self {
        Error::Config(err.to_string())
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(err: tokio::sync::oneshot::error::RecvError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<async_raft::RaftError> for Error {
    fn from(err: async_raft::RaftError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl StdError for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::result::Result<(), fmt::Error> {
        use self::Error::*;

        match *self {
            FileAccess(ref e) => write!(f, "File access error: {}", e),
            WrongObjectType(ref e) => write!(f, "Wrong objet type: {}", e),
            DataCorrupted(ref e) => write!(f, "Data corrupted: {}", e),
            ProgramLimitExceed(ref e) => write!(f, "Program limit exceed: {}", e),
            InvalidState(ref e) => write!(f, "Invalid state: {}", e),
            InvalidArgument(ref e) => write!(f, "Invalid argument: {}", e),
            Io(ref e) => write!(f, "IO error: {}", e),
            Config(ref e) => write!(f, "Config error: {}", e),
            Internal(ref e) => write!(f, "Internal error: {}", e),
            OutOfMemory => write!(f, "Out of memory."),
            RequestAborted => write!(f, "Request aborted."),
        }
    }
}
