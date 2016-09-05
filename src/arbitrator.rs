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
use std::sync::{Arc, RwLock};
use std::thread::{JoinHandle, spawn};
use std::time::Instant;

#[cfg(not(test))]
const CHUNK_TIMEOUT: u64 = 60;
#[cfg(test)]
const CHUNK_TIMEOUT: u64 = 1;

pub struct Arbitrator {
    router: ZSock,
    queue: Arc<RwLock<Vec<TimedChunk>>>,
    timer_handle: Option<JoinHandle<()>>,
    timer_comm: ZSock,
    slots: u32,
}

impl Drop for Arbitrator {
    fn drop(&mut self) {
        // Ignore failure as it means the thread has already
        // terminated.
        let _ = self.timer_comm.signal(0);
        if self.timer_handle.is_some() {
            self.timer_handle.take().unwrap().join().unwrap();
        }
    }
}

impl Arbitrator {
    pub fn new(router: ZSock, upload_slots: u32) -> Result<Arbitrator> {
        let (comm_front, comm_back) = try!(ZSys::create_pipe());
        comm_front.set_sndtimeo(Some(1000));
        comm_front.set_linger(0);
        comm_back.set_rcvtimeo(Some(1000)); // Remember that this timeout controls the Timer loop speed!
        comm_back.set_linger(0);

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
        {
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
                    self.slots += 1;
                },
                None => return Err(Error::ChunkIndex),
            }
        }

        try!(self.request());
        Ok(())
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
                try!(msg.send(&mut self.router));

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
            sink: try!(ZSock::new_push(">inproc://zfilexfer_sink")),
            comm: comm,
        })
    }

    fn run(mut self) {
        loop {
            // Terminate on ZSock signal or system signal (SIGTERM)
            if self.comm.wait().is_ok() || ZSys::is_interrupted() {
                break;
            }

            for chunk in self.chunks.read().unwrap().iter() {
                if chunk.is_expired() {
                    let msg = ZMsg::new();
                    msg.addbytes(&chunk.router_id).unwrap();
                    msg.addstr(&chunk.index.to_string()).unwrap();
                    msg.addstr("0").unwrap();
                    msg.send(&mut self.sink).unwrap();
                }
            }
        }
    }
}

struct TimedChunk {
    router_id: Vec<u8>,
    index: u64,
    timestamp: Option<Instant>,
}

impl TimedChunk {
    fn new(router_id: &[u8], index: u64) -> TimedChunk {
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
            self.timestamp.as_ref().unwrap().elapsed().as_secs() >= CHUNK_TIMEOUT
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use chunk::Chunk;
    use czmq::{ZMsg, ZSock, ZSockType, ZSys};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, RwLock};
    use std::thread::{sleep, spawn};
    use std::time::{Duration, Instant};
    use super::*;
    use super::{TimedChunk, Timer};
    use tempfile::tempfile;

    #[test]
    fn test_arbitrator_new() {
        ZSys::init();

        assert!(Arbitrator::new(ZSock::new(ZSockType::PAIR), 0).is_ok());
    }

    #[test]
    fn test_arbitrator_queue_release() {
        ZSys::init();

        let chunk = Chunk::new(Rc::new(RefCell::new(tempfile().unwrap())), 0);

        let mut arbitrator = Arbitrator::new(ZSock::new(ZSockType::ROUTER), 1).unwrap();
        assert!(arbitrator.queue(&chunk, "abc".as_bytes()).is_ok());
        assert_eq!(arbitrator.queue.read().unwrap().len(), 1);
        assert_eq!(arbitrator.slots, 0);
        assert!(arbitrator.release(&chunk, "abc".as_bytes()).is_ok());
        assert_eq!(arbitrator.queue.read().unwrap().len(), 0);
        assert_eq!(arbitrator.slots, 1);
    }

    #[test]
    fn test_arbitrator_request() {
        ZSys::init();

        let (mut client, router) = ZSys::create_pipe().unwrap();
        client.set_rcvtimeo(Some(500));

        let (comm, thread) = ZSys::create_pipe().unwrap();

        let chunks = vec![
            TimedChunk::new("abc".as_bytes(), 0),
            TimedChunk::new("abc".as_bytes(), 1),
            TimedChunk::new("abc".as_bytes(), 2),
            TimedChunk::new("def".as_bytes(), 0),
            TimedChunk::new("def".as_bytes(), 1),
            TimedChunk::new("def".as_bytes(), 2),
        ];

        {
            let mut arbitrator = Arbitrator {
                router: router,
                queue: Arc::new(RwLock::new(chunks)),
                timer_handle: None,
                timer_comm: comm,
                slots: 3,
            };

            arbitrator.request().unwrap();

            for x in 0..3 {
                let msg = ZMsg::recv(&mut client).unwrap();
                assert_eq!(&msg.popstr().unwrap().unwrap(), "abc");
                assert_eq!(&msg.popstr().unwrap().unwrap(), "CHUNK");
                assert_eq!(msg.popstr().unwrap().unwrap(), x.to_string());
            }

            assert!(client.recv_str().is_err());

            let chunk = Chunk::new(Rc::new(RefCell::new(tempfile().unwrap())), 0);
            arbitrator.release(&chunk, "abc".as_bytes()).unwrap();
            arbitrator.request().unwrap();

            let msg = ZMsg::recv(&mut client).unwrap();
            assert_eq!(&msg.popstr().unwrap().unwrap(), "def");
            assert_eq!(&msg.popstr().unwrap().unwrap(), "CHUNK");
            assert_eq!(msg.popstr().unwrap().unwrap(), "0");
        }

        thread.wait().unwrap();
    }

    #[test]
    fn test_timer_new() {
        ZSys::init();

        assert!(Timer::new(ZSock::new(ZSockType::REQ), Arc::new(RwLock::new(Vec::new()))).is_ok());
    }

    #[test]
    fn test_timer_run() {
        ZSys::init();

        let (mut client, server) = ZSys::create_pipe().unwrap();
        let (comm, thread) = ZSys::create_pipe().unwrap();
        client.set_rcvtimeo(Some(1500));
        thread.set_rcvtimeo(Some(1000));

        let mut c = TimedChunk::new("abc".as_bytes(), 0);
        c.start();

        let timer = Timer {
            chunks: Arc::new(RwLock::new(vec![
                c,
            ])),
            sink: server,
            comm: thread,
        };
        let handle = spawn(|| timer.run());

        let msg = ZMsg::recv(&mut client).unwrap();
        assert_eq!(msg.popstr().unwrap().unwrap(), "abc");
        assert_eq!(msg.popstr().unwrap().unwrap(), "0");
        assert_eq!(msg.popstr().unwrap().unwrap(), "0");

        comm.signal(0).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_chunk_is_expired() {
        let timed = TimedChunk {
            router_id: vec![97, 98, 99],
            index: 0,
            timestamp: Some(Instant::now()),
        };

        sleep(Duration::from_secs(1));

        assert!(timed.is_expired());
    }
}
