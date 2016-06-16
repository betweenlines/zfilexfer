// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use arbitrator::Arbitrator;
use chunk::Chunk;
use error::{Error, Result};
use std::collections::HashMap;
use std::fs::{create_dir_all, rename, self};
use std::path::{Path, PathBuf};

const MAX_CHUNK_ERR: u8 = 5;

pub struct File {
    path: PathBuf,
    upload_path: PathBuf,
    chunks: HashMap<usize, Chunk>,
    chunk_error_cnt: u8,
    chunk_size: usize,
}

impl File {
    fn temporary_filename<P: AsRef<Path>>(path: P) -> PathBuf {
        let mut counter: u16 = 0;
        let mut buf = path.as_ref().to_owned();

        loop {
            buf.set_file_name(&format!(".{}{}", path.as_ref().file_name().unwrap().to_str().unwrap(), counter));

            if !buf.exists() {
                return buf;
            }

            counter += 1;
        }
    }

    /// Open a local file for sending
    // pub fn open(path: &str) -> Result<File> {
    //     // Check file path is valid
    //     // Get file size
    //     let size = 0;
    //
    //     Ok(File {
    //         path: path.into(),
    //         size: size,
    //         chunks: HashMap::new(),
    //     })
    // }

    /// Create a new file container for receiving
    pub fn create<P: AsRef<Path>>(arbitrator: &mut Arbitrator, router_id: &[u8], path: P, size: usize, chunk_size: usize) -> Result<File> {
        let upload_path = Self::temporary_filename(path.as_ref());

        // Create file
        try!(create_dir_all(path.as_ref().parent().unwrap()));
        let f = try!(fs::File::create(&upload_path));
        try!(f.set_len(size as u64));

        // Split size into chunks and queue
        let mut chunks = HashMap::new();
        let mut size_ctr = size;
        let mut index = 0;
        while size_ctr > 0 {
            let chunk = try!(Chunk::create(&upload_path, index));
            try!(arbitrator.queue(&chunk, router_id));
            chunks.insert(index, chunk);

            index += 1;
            size_ctr -= chunk_size;
        }

        Ok(File {
            path: path.as_ref().to_owned(),
            upload_path: upload_path,
            chunks: chunks,
            chunk_error_cnt: 0,
            chunk_size: chunk_size,
        })
    }

    // pub fn send(&self, sock: &ZSock) -> Result<()> {
    //     // if self.router_id.is_some() {
    //     //     return Err(Error::ModeSend);
    //     // }
    //
    //     loop {
    //         // Send req to server
    //         // Chunk file
    //         // Listen for chunk req's and send
    //         // Listen for OK/ERR
    //         break;
    //     }
    //
    //     Ok(())
    // }

    pub fn recv(&mut self, router_id: &[u8], index: usize, chunk_data: Vec<u8>) -> Result<()> {
        let chunk = try!(self.chunks.get_mut(&index).ok_or(Error::ChunkIndex));
        try!(chunk.recv(router_id, chunk_data, self.chunk_size));

        Ok(())
    }

    pub fn sink(&mut self, arbitrator: &mut Arbitrator, router_id: &[u8], index: usize, success: bool) -> Result<()> {
        if success {
            {
                let chunk = try!(self.chunks.get_mut(&index).ok_or(Error::ChunkIndex));
                try!(arbitrator.release(chunk, router_id));
            }
            self.chunks.remove(&index);
        } else if self.chunk_error_cnt < MAX_CHUNK_ERR {
            let chunk = try!(self.chunks.get(&index).ok_or(Error::ChunkIndex));
            try!(arbitrator.queue(chunk, router_id));
            self.chunk_error_cnt += 1;
        }

        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.chunks.len() == 0
    }

    pub fn is_error(&self) -> bool {
        self.chunk_error_cnt >= MAX_CHUNK_ERR
    }

    pub fn save(&self) -> Result<()> {
        // XXX Check checksum
        try!(rename(&self.upload_path, &self.path));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arbitrator::Arbitrator;
    use czmq::{ZSock, ZSockType, ZSys};
    use std::fs;
    use std::path::Path;
    use std::rc::Rc;
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_temporary_filename() {
        assert_eq!(File::temporary_filename("/path/to/file"), Path::new("/path/to/.file0"));

        let tempdir = TempDir::new("file_test_temporary_filename").unwrap();
        let path = tempdir.path().to_str().unwrap();
        fs::File::create(&format!("{}/.file0", path)).unwrap();

        assert_eq!(File::temporary_filename(format!("{}/file", path)), Path::new(&format!("{}/.file1", path)));
    }

    #[test]
    fn test_create() {
        let tempdir = TempDir::new("file_test_new").unwrap();
        let mut arbitrator = Arbitrator::new(Rc::new(ZSock::new(ZSockType::ROUTER)), 0).unwrap();
        assert!(File::create(&mut arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 0, 1).is_ok());
    }

    #[test]
    fn test_recv() {
        ZSys::init();

        let tempdir = TempDir::new("file_test_recv").unwrap();
        let mut arbitrator = Arbitrator::new(Rc::new(ZSock::new(ZSockType::ROUTER)), 0).unwrap();
        let mut file = File::create(&mut arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 1, 1).unwrap();
        assert!(file.recv(&Vec::new(), 0, Vec::new()).is_ok());
    }

    #[test]
    fn test_sink() {
        ZSys::init();

        let tempdir = TempDir::new("file_test_recv").unwrap();
        let mut arbitrator = Arbitrator::new(Rc::new(ZSock::new(ZSockType::ROUTER)), 0).unwrap();
        let mut file = File::create(&mut arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 1, 1).unwrap();

        for _ in 0..6 {
            file.sink(&mut arbitrator, "abc".as_bytes(), 0, false).unwrap();
        }

        assert!(file.is_error());
        assert!(file.sink(&mut arbitrator, "abc".as_bytes(), 0, true).is_ok());
        assert!(file.is_complete());
    }

    #[test]
    fn test_save() {
        ZSys::init();

        let tempdir = TempDir::new("file_test_save").unwrap();
        let mut tmp_path = tempdir.path().to_path_buf();
        tmp_path.push(".file0");
        let mut path = tempdir.path().to_path_buf();
        path.push("file");

        let mut arbitrator = Arbitrator::new(Rc::new(ZSock::new(ZSockType::ROUTER)), 0).unwrap();
        let file = File::create(&mut arbitrator, "abc".as_bytes(), &path, 0, 1).unwrap();

        assert!(tmp_path.exists());
        assert!(!path.exists());
        assert!(file.save().is_ok());
        assert!(!tmp_path.exists());
        assert!(path.exists());
    }
}
