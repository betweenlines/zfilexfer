// Copyright 2015-2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use chunk::Chunk;
use czmq::{ZMsg, ZSock, ZSys};
use error::{Error, Result};
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::thread::{JoinHandle, spawn};
use std::time::Instant;

const CHUNK_TIMEOUT: u64 = 60;

pub struct Arbitrator {
    router: Rc<ZSock>,
    queue: Arc<RwLock<Vec<TimedChunk>>>,
    timer_handle: Option<JoinHandle<()>>,
    timer_comm: ZSock,
    slots: u32,
}

impl Drop for Arbitrator {
    fn drop(&mut self) {
        self.timer_comm.signal(0).unwrap();
        self.timer_handle.take().unwrap().join().unwrap();
    }
}

impl Arbitrator {
    pub fn new(router: Rc<ZSock>, upload_slots: u32) -> Result<Arbitrator> {
        let (comm_front, comm_back) = try!(ZSys::create_pipe());
        comm_front.set_sndtimeo(Some(1000));
        comm_back.set_rcvtimeo(Some(1000)); // Remember that this timeout controls the Timer loop speed!

        let lock = Arc::new(RwLock::new(Vec::new()));
        let timer = try!(Timer::new(comm_back, lock.clone()));

        Ok(Arbitrator {
            router: router,
            queue: lock,
            timer_handle: Some(spawn(move|| timer.run())),
            timer_comm: comm_front,
            slots: upload_slots,
        })
    }

    pub fn queue(&mut self, chunk: &Chunk, router_id: &[u8]) -> Result<()> {
        let timed_chunk = TimedChunk::new(router_id, chunk.get_index());
        {
            let mut writer = self.queue.write().unwrap();
            writer.push(timed_chunk);
        }

        try!(self.request());
        Ok(())
    }

    pub fn release(&mut self, chunk: &Chunk, router_id: &[u8]) -> Result<()> {
        let router_id = router_id.to_vec();
        let mut queue = self.queue.write().unwrap();
        let mut index: Option<usize> = None;
        let mut x = 0;
        for c in queue.iter_mut() {
            if c.router_id == router_id && c.index == chunk.get_index() {
                index = Some(x);
                break;
            }

            x += 1;
        }

        match index {
            Some(i) => {
                queue.remove(i);
                Ok(())
            },
            None => Err(Error::ChunkIndex),
        }
    }

    fn request(&mut self) -> Result<()> {
        for chunk in self.queue.write().unwrap().iter_mut() {
            if self.slots == 0 {
                break;
            }

            if !chunk.is_started() {
                self.slots -= 1;

                let msg = ZMsg::new();
                try!(msg.addbytes(&chunk.router_id));
                try!(msg.addstr("CHUNK"));
                try!(msg.addstr(&chunk.index.to_string()));
                try!(msg.send(&*self.router));

                chunk.start();
            }
        }

        Ok(())
    }
}

struct Timer {
    chunks: Arc<RwLock<Vec<TimedChunk>>>,
    sink: ZSock,
    comm: ZSock,
}

impl Timer {
    fn new(comm: ZSock, chunks: Arc<RwLock<Vec<TimedChunk>>>) -> Result<Timer> {
        Ok(Timer {
            chunks: chunks,
            sink: try!(ZSock::new_push("inproc://zfilexfer_sink")),
            comm: comm,
        })
    }

    fn run(self) {
        loop {
            // Terminate on signal
            if self.comm.wait().is_ok() {
                break;
            }

            for chunk in self.chunks.read().unwrap().iter() {
                if chunk.is_expired() {
                    let msg = ZMsg::new();
                    msg.addbytes(&chunk.router_id).unwrap();
                    msg.addstr(&chunk.index.to_string()).unwrap();
                    msg.addstr("0").unwrap();
                    msg.send(&self.sink).unwrap();
                }
            }
        }
    }
}

struct TimedChunk {
    router_id: Vec<u8>,
    index: usize,
    timestamp: Option<Instant>,
}

impl TimedChunk {
    fn new(router_id: &[u8], index: usize) -> TimedChunk {
        TimedChunk {
            router_id: router_id.to_vec(),
            index: index,
            timestamp: None,
        }
    }

    fn start(&mut self) {
        self.timestamp = Some(Instant::now());
    }

    fn is_started(&self) -> bool {
        self.timestamp.is_some()
    }

    fn is_expired(&self) -> bool {
        if self.timestamp.is_some() {
            self.timestamp.as_ref().unwrap().elapsed().as_secs() > CHUNK_TIMEOUT
        } else {
            false
        }
    }
}
