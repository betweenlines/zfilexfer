// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use czmq::{ZMsg, ZSock};
use error::Result;
use std::cell::RefCell;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::rc::Rc;

pub struct Chunk {
    fh: Rc<RefCell<fs::File>>,
    index: u64,
}

impl Chunk {
    pub fn new(file: Rc<RefCell<fs::File>>, index: u64) -> Chunk {
        Chunk {
            fh: file,
            index: index,
        }
    }

    pub fn send(&mut self, sock: &mut ZSock, chunk_size: u64, file_size: u64) -> Result<()> {
        let start = chunk_size * self.index;
        let buf_size = if (start + chunk_size) > file_size {
            file_size - start
        } else {
            chunk_size
        };

        let mut fh = self.fh.borrow_mut();
        try!(fh.seek(SeekFrom::Start(start)));

        let mut buf = Vec::with_capacity(buf_size as usize);
        unsafe { buf.set_len(buf_size as usize); }
        try!(fh.read_exact(&mut buf));

        let msg = ZMsg::new();
        try!(msg.addstr("CHUNK"));
        try!(msg.addstr(&self.index.to_string()));
        try!(msg.addbytes(&buf));
        try!(msg.send(sock));
        Ok(())
    }

    pub fn recv(&mut self, router_id: &[u8], data: Vec<u8>, chunk_size: u64) -> Result<()> {
        let sock = try!(ZSock::new_push(">inproc://zfilexfer_sink"));
        sock.set_sndtimeo(Some(1000));

        self.do_recv(router_id, data, chunk_size, sock)
    }

    pub fn do_recv(&mut self, router_id: &[u8], data: Vec<u8>, chunk_size: u64, mut sock: ZSock) -> Result<()> {
        let result = || -> Result<()> {
            let mut fh = self.fh.borrow_mut();
            try!(fh.seek(SeekFrom::Start((self.index * chunk_size) as u64)));
            try!(fh.write_all(&data));
            Ok(())
        }();

        let msg = ZMsg::new();
        try!(msg.addbytes(router_id));
        try!(msg.addstr(&self.index.to_string()));
        try!(msg.addstr(if result.is_ok() { "1" } else { "0" }));
        try!(msg.send(&mut sock));

        Ok(())
    }

    pub fn get_index(&self) -> u64 {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use czmq::{ZMsg, ZSys};
    use std::cell::RefCell;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::rc::Rc;
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_recv() {
        ZSys::init();

        let tempdir = TempDir::new("chunk_test_create_recv").unwrap();
        let path = format!("{}/test", tempdir.path().to_str().unwrap());

        let fh = Rc::new(RefCell::new(OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap()));
        fh.borrow().set_len(6).unwrap();

        let (thread, mut sink) = ZSys::create_pipe().unwrap();
        let mut chunk = Chunk::new(fh.clone(), 1);
        chunk.do_recv("abc".as_bytes(), "abc".as_bytes().to_vec(), 3, thread).unwrap();

        let msg = ZMsg::recv(&mut sink).unwrap();
        let _ = msg.popstr();
        let _ = msg.popstr();
        assert_eq!(msg.popstr().unwrap().unwrap(), "1");

        let mut content = Vec::new();
        fh.borrow_mut().seek(SeekFrom::Start(0)).unwrap();
        fh.borrow_mut().read_to_end(&mut content).unwrap();
        assert_eq!(content, vec![0, 0, 0, 97, 98, 99]);
    }

    #[test]
    fn test_send() {
        ZSys::init();

        let tempdir = TempDir::new("chunk_test_create_recv").unwrap();
        let path = format!("{}/test", tempdir.path().to_str().unwrap());

        let mut fh = OpenOptions::new().read(true).write(true).create(true).open(&path).unwrap();
        fh.write_all("abc".as_bytes()).unwrap();

        let (mut client, mut server) = ZSys::create_pipe().unwrap();

        let mut chunk = Chunk::new(Rc::new(RefCell::new(fh)), 0);
        chunk.send(&mut client, 2, 3).unwrap();

        let msg = ZMsg::recv(&mut server).unwrap();
        assert_eq!(&msg.popstr().unwrap().unwrap(), "CHUNK");
        assert_eq!(&msg.popstr().unwrap().unwrap(), "0");
        assert_eq!(&msg.popstr().unwrap().unwrap(), "ab");
    }
}
