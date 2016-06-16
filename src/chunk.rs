// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use czmq::{ZMsg, ZSock};
use error::{Error, Result};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread::{JoinHandle, spawn};

pub struct Chunk {
    path: PathBuf,
    index: usize,
    join_handle: Option<JoinHandle<()>>,
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if self.join_handle.is_some() {
            self.join_handle.take().unwrap().join().unwrap();
        }
    }
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
    pub fn create<P: AsRef<Path>>(path: P, index: usize) -> Result<Chunk> {
        let path = path.as_ref();

        if !path.exists() || !path.is_file() {
            return Err(Error::InvalidFilePath);
        }

        Ok(Chunk {
            path: path.to_owned(),
            index: index,
            join_handle: None,
        })
    }

    // pub fn send(&self, sock: &ZSock) -> Result<()> {
    //     // Send chunk to sock
    //     Ok(())
    // }

    pub fn recv(&mut self, router_id: &[u8], data: Vec<u8>, chunk_size: usize) -> Result<()> {
        let sock = try!(ZSock::new_push(">inproc://zfilexfer_sink"));
        sock.set_sndtimeo(Some(1000));

        self.do_recv(router_id, data, chunk_size, sock)
    }

    pub fn do_recv(&mut self, router_id: &[u8], data: Vec<u8>, chunk_size: usize, sock: ZSock) -> Result<()> {
        if self.join_handle.is_some() {
            self.join_handle.take().unwrap().join().unwrap();
        }

        let path = self.path.clone();
        let chunk_index = self.index.clone();
        let router_id = router_id.to_vec();

        self.join_handle = Some(spawn(move|| {
            let result = || -> Result<()> {
                let mut file = try!(OpenOptions::new().write(true).create(false).open(&path));
                try!(file.seek(SeekFrom::Start((chunk_index * chunk_size) as u64)));
                try!(file.write_all(&data));
                Ok(())
            }();

            // If we can't send a message, it isn't recoverable by
            // the application, so panicking is appropriate.
            let msg = ZMsg::new();
            msg.addbytes(&router_id).unwrap();
            msg.addstr(&chunk_index.to_string()).unwrap();
            msg.addstr(if result.is_ok() { "1" } else { "0" }).unwrap();
            msg.send(&sock).unwrap();
        }));

        Ok(())
    }

    pub fn get_index(&self) -> usize {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use czmq::{ZMsg, ZSys};
    use std::fs::{File, OpenOptions};
    use std::io::Read;
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_create() {
        assert!(Chunk::create("/fake/path", 0).is_err());

        let tempdir = TempDir::new("chunk_test_create").unwrap();
        let path = format!("{}/test", tempdir.path().to_str().unwrap());
        File::create(&path).unwrap();

        assert!(Chunk::create(&path, 0).is_ok());
    }

    #[test]
    fn test_recv() {
        ZSys::init();

        let tempdir = TempDir::new("chunk_test_create").unwrap();
        let path = format!("{}/test", tempdir.path().to_str().unwrap());

        let mut fh = OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap();
        fh.set_len(6).unwrap();

        let (thread, sink) = ZSys::create_pipe().unwrap();
        let mut chunk = Chunk::create(&path, 1).unwrap();
        chunk.do_recv("abc".as_bytes(), "abc".as_bytes().to_vec(), 3, thread).unwrap();

        let msg = ZMsg::recv(&sink).unwrap();
        let _ = msg.popstr();
        let _ = msg.popstr();
        assert_eq!(msg.popstr().unwrap().unwrap(), "1");

        let mut content = Vec::new();
        fh.read_to_end(&mut content).unwrap();
        assert_eq!(content, vec![0, 0, 0, 97, 98, 99]);
    }
}
