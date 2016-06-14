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

pub struct File {
    path: PathBuf,
    upload_path: PathBuf,
    chunks: HashMap<usize, Chunk>,
}

impl File {
    fn get_unique_filename<P: AsRef<Path>>(path: P) -> PathBuf {
        let mut counter: u16 = 0;
        let mut buf = path.as_ref().to_owned();
        let filename = buf.file_name().unwrap().to_str().unwrap().to_string();

        loop {
            if !buf.exists() {
                return buf;
            }

            buf.set_file_name(&format!("{}{}", filename, counter));
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
    pub fn create<P: AsRef<Path>>(arbitrator: &Arbitrator, path: P, size: usize, chunk_size: usize) -> Result<File> {
        let mut upload_path = path.as_ref().to_owned();
        let filename = path.as_ref().file_name().unwrap().to_str().unwrap();
        upload_path.set_file_name(&format!(".{}", filename));
        let upload_path = Self::get_unique_filename(path.as_ref());

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
            try!(arbitrator.queue(&chunk));
            chunks.insert(index, chunk);

            index += 1;
            size_ctr -= chunk_size;
        }

        Ok(File {
            path: path.as_ref().to_owned(),
            upload_path: upload_path,
            chunks: chunks,
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

    pub fn recv(&self, router_id: &[u8], index: usize, chunk_data: Vec<u8>) -> Result<()> {
        let chunk = try!(self.chunks.get(&index).ok_or(Error::ChunkIndex));
        try!(chunk.recv(router_id, chunk_data));

        Ok(())
    }

    pub fn sink(&mut self, arbitrator: &Arbitrator, index: usize, success: bool) -> Result<()> {
        if success {
            self.chunks.remove(&index);
            try!(arbitrator.release());
        } else {
            let chunk = try!(self.chunks.get(&index).ok_or(Error::InvalidRequest));
            try!(arbitrator.queue(chunk));
        }

        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.chunks.len() == 0
    }

    pub fn save(&self) -> Result<()> {
        try!(rename(&self.upload_path, &self.path));
        Ok(())
    }
}
