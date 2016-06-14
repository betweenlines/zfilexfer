// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use czmq::ZSock;
use error::Result;
use std::path::{Path, PathBuf};

pub struct Chunk {
    path: PathBuf,
    id: usize,
}

impl Chunk {
    // pub fn open(path: &str, id: u64) -> Result<Chunk> {
    //     // Check file path is valid
    //
    //     Ok(Chunk {
    //         path: path.into(),
    //         id: id,
    //     })
    // }
    //
    pub fn create<P: AsRef<Path>>(path: P, id: usize) -> Result<Chunk> {
        let path = path.as_ref();

        // Check file path is valid

        Ok(Chunk {
            path: path.to_owned(),
            id: id,
        })
    }

    // pub fn send(&self, sock: &ZSock) -> Result<()> {
    //     // Send chunk to sock
    //     Ok(())
    // }

    pub fn recv(&self, router_id: &[u8], data: Vec<u8>) -> Result<()> {
        // Check for chunk file in self.path for previous id
        // E.g. for chunk id 3, look for file {self.path}/2.chunk
        // If found:
        //     Append to existing file
        //     Move file to current id.chunk, e.g. 2.chunk => 3.chunk
        // Else:
        //     Create new chunk file, e.g. 3.chunk
        Ok(())
    }
}
