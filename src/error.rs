// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use czmq;
use std::{convert, error, fmt, io, result, str};
use zdaemon;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ChunkIndex,
    Czmq(czmq::Error),
    InvalidRequest,
    Io(io::Error),
    ModeRecv,
    ModeSend,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ChunkIndex => write!(f, "Chunk index not in file"),
            Error::Czmq(ref e) => write!(f, "CZMQ error: {}", e),
            Error::InvalidRequest => write!(f, "Invalid request"),
            Error::Io(ref e) => write!(f, "IO error: {}", e),
            Error::ModeRecv => write!(f, "Struct is in wrong mode for receiving"),
            Error::ModeSend => write!(f, "Struct is in wrong mode for sending"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ChunkIndex => "Chunk index not in file",
            Error::Czmq(ref e) => e.description(),
            Error::InvalidRequest => "Invalid request",
            Error::Io(ref e) => e.description(),
            Error::ModeRecv => "Struct is in wrong mode for receiving",
            Error::ModeSend => "Struct is in wrong mode for sending",
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

impl convert::Into<zdaemon::Error> for Error {
    fn into(self) -> zdaemon::Error {
        zdaemon::Error::Generic(Box::new(self))
    }
}
