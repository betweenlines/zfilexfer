// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use czmq;
use rustc_serialize::json;
use std::{convert, error, fmt, io, result, str};
use zdaemon;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ChunkFail,
    ChunkIndex,
    Czmq(czmq::Error),
    FailChecksum,
    FileFail,
    InvalidFileOpts,
    InvalidFilePath,
    InvalidReply,
    InvalidRequest,
    Io(io::Error),
    JsonEncoder(json::EncoderError),
    JsonDecoder(json::DecoderError),
    ModeRecv,
    ModeSend,
    UploadError(String),
}

unsafe impl Send for Error {}
unsafe impl Sync for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ChunkFail => write!(f, "Failed to save chunk to file"),
            Error::ChunkIndex => write!(f, "Chunk index not in file"),
            Error::Czmq(ref e) => write!(f, "CZMQ error: {}", e),
            Error::FailChecksum => write!(f, "Uploaded file does not match expected CRC"),
            Error::FileFail => write!(f, "Failed to upload file"),
            Error::InvalidFileOpts => write!(f, "Invalid file options"),
            Error::InvalidFilePath => write!(f, "Path does not exist or is not a file"),
            Error::InvalidReply => write!(f, "Invalid reply"),
            Error::InvalidRequest => write!(f, "Invalid request"),
            Error::Io(ref e) => write!(f, "IO error: {}", e),
            Error::JsonEncoder(ref e) => write!(f, "JSON encoder error: {}", e),
            Error::JsonDecoder(ref e) => write!(f, "JSON decoder error: {}", e),
            Error::ModeRecv => write!(f, "Struct is in wrong mode for receiving"),
            Error::ModeSend => write!(f, "Struct is in wrong mode for sending"),
            Error::UploadError(ref e) => write!(f, "Could not upload file: {}", e),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ChunkFail => "Failed to save chunk to file",
            Error::ChunkIndex => "Chunk index not in file",
            Error::Czmq(ref e) => e.description(),
            Error::FailChecksum => "Uploaded file does not match expected CRC",
            Error::FileFail => "Failed to upload file",
            Error::InvalidFileOpts => "Invalid file options",
            Error::InvalidFilePath => "Path does not exist or is not a file",
            Error::InvalidReply => "Invalid reply",
            Error::InvalidRequest => "Invalid request",
            Error::Io(ref e) => e.description(),
            Error::JsonEncoder(ref e) => e.description(),
            Error::JsonDecoder(ref e) => e.description(),
            Error::ModeRecv => "Struct is in wrong mode for receiving",
            Error::ModeSend => "Struct is in wrong mode for sending",
            Error::UploadError(ref e) => e,
        }
    }
}

impl convert::From<czmq::Error> for Error {
    fn from(err: czmq::Error) -> Error {
        Error::Czmq(err)
    }
}

impl convert::From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl convert::From<json::EncoderError> for Error {
    fn from(err: json::EncoderError) -> Error {
        Error::JsonEncoder(err)
    }
}

impl convert::From<json::DecoderError> for Error {
    fn from(err: json::DecoderError) -> Error {
        Error::JsonDecoder(err)
    }
}

impl convert::Into<zdaemon::Error> for Error {
    fn into(self) -> zdaemon::Error {
        zdaemon::Error::Generic(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use czmq::{ZSock, SocketType, ZSys};
    use rustc_serialize::json::{DecoderError, EncoderError};
    use std::fs::metadata;
    use super::*;
    use zdaemon;

    #[test]
    fn test_convert_czmq() {
        ZSys::init();

        let sock = ZSock::new(SocketType::REQ);
        let e = sock.recv_str().unwrap_err();
        Error::from(e);
    }

    #[test]
    fn test_convert_io() {
        if let Err(e) = metadata("/fake/path") {
            Error::from(e);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_convert_json_encode() {
        let e = EncoderError::BadHashmapKey;
        Error::from(e);
    }

    #[test]
    fn test_convert_json_decode() {
        let e = DecoderError::EOF;
        Error::from(e);
    }

    #[test]
    fn test_convert_zdaemon() {
        let e = Error::ChunkFail;
        let _: zdaemon::Error = e.into();
    }
}
