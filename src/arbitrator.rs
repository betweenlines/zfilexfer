// Copyright 2015-2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use chunk::Chunk;
use czmq::ZSock;
use error::Result;
use std::rc::Rc;

pub struct Arbitrator {
    router: Rc<ZSock>,
}

impl Arbitrator {
    pub fn new(router: Rc<ZSock>) -> Arbitrator {
        Arbitrator {
            router: router,
        }
    }

    pub fn queue(&self, chunk: &Chunk) -> Result<()> {
        Ok(())
    }

    pub fn release(&self) -> Result<()> {
        Ok(())
    }

    // fn run(&self) -> Result<()> {
    //     Ok(())
    // }
}

// struct ArbitratorTimer {
//     chunks: Vec<ChunkTimer>,
// }
//
// impl ArbitratorTimer {
//     fn new() {
//
//     }
//
//     fn add() {
//
//     }
//
//     fn delete() {
//
//     }
//
//     fn count() {
//
//     }
// }
