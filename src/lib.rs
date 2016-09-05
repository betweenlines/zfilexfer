// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

extern crate crc;
extern crate czmq;
extern crate rustc_serialize;
#[cfg(test)]
extern crate tempdir;
#[cfg(test)]
extern crate tempfile;
extern crate zdaemon;

mod arbitrator;
mod chunk;
mod error;
mod file;
mod server;

pub use error::Error;
pub use file::{File, Options as FileOptions};
pub use server::Server;
