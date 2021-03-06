// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use arbitrator::Arbitrator;
use chunk::Chunk;
use crc::{crc64, Hasher64};
use czmq::{ZMsg, ZSock};
use error::{Error, Result};
use rustc_serialize::json;
use std::cell::{RefMut, RefCell};
use std::collections::HashMap;
use std::fs::{create_dir_all, rename, self};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::rc::Rc;

const CHUNK_SIZE: u64 = 1024; // 1Kb
const MAX_CHUNK_ERR: u8 = 5;

pub struct File {
    fh: Rc<RefCell<fs::File>>,
    path: Option<PathBuf>,
    upload_path: Option<PathBuf>,
    size: u64,
    crc: u64,
    chunks: HashMap<u64, Chunk>,
    chunk_error_cnt: u8,
    chunk_size: u64,
    options: FileOptions,
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

    fn calc_crc(mut fh: RefMut<fs::File>) -> Result<u64> {
        let mut buf = [0; 1024];
        let mut digest = crc64::Digest::new(crc64::ECMA);

        try!(fh.seek(SeekFrom::Start(0)));
        while try!(fh.read(&mut buf)) > 0 {
            digest.write(&buf);
        }

        Ok(digest.sum64())
    }

    /// Open a local file for sending
    pub fn open<P: AsRef<Path>>(path: P, options: Option<&[Options]>) -> Result<File> {
        // Check file exists
        if !path.as_ref().exists() || !path.as_ref().is_file() {
            return Err(Error::InvalidFilePath);
        }

        let fh = try!(fs::File::open(&path));
        Self::open_file(fh, options)
    }

    /// Wrap a local file for sending
    pub fn open_file(fh: fs::File, options: Option<&[Options]>) -> Result<File> {
        let meta = try!(fh.metadata());
        let fh = Rc::new(RefCell::new(fh));
        let crc = try!(Self::calc_crc(fh.borrow_mut()));

        let mut file = File {
            fh: fh.clone(),
            path: None,
            upload_path: None,
            size: meta.len(),
            crc: crc,
            chunks: HashMap::new(),
            chunk_error_cnt: 0,
            chunk_size: CHUNK_SIZE,
            options: FileOptions::new(options),
        };

        if let Some(size) = file.options.chunk_size {
            file.chunk_size = size;
        }

        // Create chunks
        let mut size_ctr = file.size as i64;
        let mut index = 0;
        while size_ctr > 0 {
            let chunk = Chunk::new(fh.clone(), index);
            file.chunks.insert(index, chunk);

            index += 1;
            size_ctr -= file.chunk_size as i64;
        }

        Ok(file)
    }

    /// Create a new file container from path for receiving
    pub fn create<P: AsRef<Path>>(arbitrator: &mut Arbitrator,
                                  router_id: &[u8],
                                  path: P,
                                  size: u64,
                                  crc: u64,
                                  chunk_size: u64,
                                  options: &str) -> Result<File> {

        let upload_path = Self::temporary_filename(path.as_ref());

        // Create file
        try!(create_dir_all(path.as_ref().parent().unwrap()));
        let fh = try!(fs::OpenOptions::new().create(true).read(true).write(true).open(&upload_path));
        try!(fh.set_len(size as u64));

        Self::create_file(arbitrator, router_id, fh, &upload_path, path, size, crc, chunk_size, options)
    }

    /// Create a new file container for receiving
    pub fn create_file<P: AsRef<Path>, Q: AsRef<Path>>(arbitrator: &mut Arbitrator,
                                                       router_id: &[u8],
                                                       fh: fs::File,
                                                       fh_path: P,
                                                       path: Q,
                                                       size: u64,
                                                       crc: u64,
                                                       chunk_size: u64,
                                                       options: &str) -> Result<File> {

        let fh = Rc::new(RefCell::new(fh));

        // Split size into chunks and queue
        let mut chunks = HashMap::new();
        let mut size_ctr = size as i64;
        let mut index = 0;
        while size_ctr > 0 {
            let chunk = Chunk::new(fh.clone(), index);
            try!(arbitrator.queue(&chunk, router_id));
            chunks.insert(index, chunk);

            index += 1;
            size_ctr -= chunk_size as i64;
        }

        // Decode options
        let options = try!(FileOptions::decode(options));

        Ok(File {
            fh: fh,
            path: Some(path.as_ref().to_owned()),
            upload_path: Some(fh_path.as_ref().to_owned()),
            size: size,
            crc: crc,
            chunks: chunks,
            chunk_error_cnt: 0,
            chunk_size: chunk_size,
            options: options,
        })
    }

    pub fn send<P: AsRef<Path>>(&mut self, sock: &mut ZSock, remote_path: P) -> Result<()> {
        let msg = ZMsg::new();
        try!(msg.addstr("NEW"));
        try!(msg.addstr(remote_path.as_ref().to_str().unwrap()));
        let meta = try!(self.fh.borrow().metadata());
        try!(msg.addstr(&meta.len().to_string()));
        try!(msg.addstr(&self.crc.to_string()));
        try!(msg.addstr(&self.chunk_size.to_string()));
        try!(msg.addstr(&try!(self.options.encode())));
        try!(msg.send(sock));

        loop {
            let msg = try!(ZMsg::recv(sock));

            match try!(msg.popstr().unwrap().or(Err(Error::InvalidReply))).as_ref() {
                "Ok" => return Ok(()),
                "Err" => return Err(Error::UploadError(msg.popstr().unwrap().unwrap())),
                "CHUNK" => {
                    let index = msg.popstr().unwrap().unwrap().parse::<u64>().unwrap();
                    match self.chunks.get_mut(&index) {
                        Some(chunk) => try!(chunk.send(sock, self.chunk_size, self.size)),
                        None => return Err(Error::ChunkIndex),
                    }
                },
                _ => unreachable!(),
            }
        }
    }

    pub fn recv(&mut self, router_id: &[u8], index: u64, chunk_data: Vec<u8>) -> Result<()> {
        let chunk = try!(self.chunks.get_mut(&index).ok_or(Error::ChunkIndex));
        try!(chunk.recv(router_id, chunk_data, self.chunk_size));

        Ok(())
    }

    pub fn sink(&mut self, arbitrator: &mut Arbitrator, router_id: &[u8], index: u64, success: bool) -> Result<()> {
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
        if self.crc != try!(Self::calc_crc(self.fh.borrow_mut())) {
            return Err(Error::FailChecksum);
        }

        let path = self.path.as_ref().unwrap();
        let upload_path = self.upload_path.as_ref().unwrap();

        // Backup existing file
        if self.options.backup_existing.is_some() && self.fh.borrow().metadata().is_ok() {
            let suffix = self.options.backup_existing.as_ref().unwrap();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            let mut backup_path = path.clone();
            backup_path.set_file_name(&format!("{}{}", file_name, suffix));
            try!(rename(path, backup_path));
        }

        try!(rename(upload_path, path));
        Ok(())
    }
}

pub enum Options {
    BackupExisting(String),
    ChunkSize(u64),
}

#[derive(RustcDecodable, RustcEncodable)]
struct FileOptions {
    backup_existing: Option<String>,
    chunk_size: Option<u64>,
}

impl FileOptions {
    fn new(options: Option<&[Options]>) -> FileOptions {
        let mut opts = FileOptions {
            backup_existing: None,
            chunk_size: None,
        };

        if let Some(options) = options {
            for opt in options {
                match opt {
                    &Options::BackupExisting(ref suffix) => opts.backup_existing = Some(suffix.to_string()),
                    &Options::ChunkSize(size) => opts.chunk_size = Some(size),
                }
            }
        }

        opts
    }

    fn decode(encoded: &str) -> Result<FileOptions> {
        let options = try!(json::decode(encoded));
        Ok(options)
    }

    fn encode(&self) -> Result<String> {
        Ok(try!(json::encode(&self)))
    }
}

#[cfg(test)]
mod tests {
    use arbitrator::Arbitrator;
    use czmq::{ZMsg, ZSock, SocketType, ZSys};
    use std::cell::RefCell;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::thread::spawn;
    use super::*;
    use super::FileOptions;
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
    fn test_calc_crc() {
        let tempdir = TempDir::new("file_test_temporary_filename").unwrap();
        let path = format!("{}/.file0", tempdir.path().to_str().unwrap());
        let fh = RefCell::new(fs::OpenOptions::new().create(true).read(true).write(true).open(&path).unwrap());
        let mut file = fh.borrow_mut();
        file.write_all(b"12345").unwrap();

        assert_eq!(File::calc_crc(file).unwrap(), 16742651521893322043);
    }

    #[test]
    fn test_create_recv() {
        let tempdir = TempDir::new("file_test_new_recv").unwrap();
        let mut arbitrator = Arbitrator::new(ZSock::new(SocketType::ROUTER), 0).unwrap();
        let mut file = File::create(&mut arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 1, 0, 1, "{}").unwrap();
        assert!(file.recv(&Vec::new(), 0, Vec::new()).is_ok());
    }

    #[test]
    fn test_open_send() {
        ZSys::init();

        let tempdir = TempDir::new("file_test_new_recv").unwrap();
        let local_path = format!("{}/local_file.txt", tempdir.path().to_str().unwrap());
        let remote_path = format!("{}/remote_file.txt", tempdir.path().to_str().unwrap());
        let remote_path_clone = remote_path.clone();
        let mut fs_file = fs::File::create(&local_path).unwrap();
        fs_file.write_all("abc".as_bytes()).unwrap();

        let (mut client, mut server) = ZSys::create_pipe().unwrap();
        client.set_rcvtimeo(Some(500));
        server.set_rcvtimeo(Some(500));

        let handle = spawn(move|| {
            let msg = ZMsg::recv(&mut server).unwrap();
            assert_eq!(&msg.popstr().unwrap().unwrap(), "NEW");
            assert_eq!(&msg.popstr().unwrap().unwrap(), &remote_path_clone);
            assert_eq!(&msg.popstr().unwrap().unwrap(), "3");
            assert_eq!(&msg.popstr().unwrap().unwrap(), "5336943202215289992");
            assert_eq!(&msg.popstr().unwrap().unwrap(), "2");
            assert_eq!(&msg.popstr().unwrap().unwrap(), "{\"backup_existing\":null,\"chunk_size\":2}");

            let msg = ZMsg::new();
            msg.addstr("CHUNK").unwrap();
            msg.addstr("1").unwrap();
            msg.send(&mut server).unwrap();

            let msg = ZMsg::recv(&mut server).unwrap();
            assert_eq!(&msg.popstr().unwrap().unwrap(), "CHUNK");
            assert_eq!(&msg.popstr().unwrap().unwrap(), "1");
            assert_eq!(&msg.popstr().unwrap().unwrap(), "c");

            let msg = ZMsg::new();
            msg.addstr("Ok").unwrap();
            msg.send(&mut server).unwrap();
        });

        let mut file = File::open(&local_path, Some(&[Options::ChunkSize(2)])).unwrap();
        file.send(&mut client, &remote_path).unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_sink() {
        ZSys::init();

        let tempdir = TempDir::new("file_test_recv").unwrap();
        let mut arbitrator = Arbitrator::new(ZSock::new(SocketType::ROUTER), 0).unwrap();
        let mut file = File::create(&mut arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 1, 0, 1, "{}").unwrap();

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

        let mut arbitrator = Arbitrator::new(ZSock::new(SocketType::ROUTER), 0).unwrap();
        let file = File::create(&mut arbitrator, "abc".as_bytes(), &path, 0, 0, 1, "{}").unwrap();

        assert!(tmp_path.exists());
        assert!(!path.exists());
        assert!(file.save().is_ok());
        assert!(!tmp_path.exists());
        assert!(path.exists());
    }

    #[test]
    fn test_file_options() {
        let options = FileOptions::new(Some(&[Options::BackupExisting("_moo".into()), Options::ChunkSize(123)]));
        let encoded = options.encode().unwrap();
        let decoded = FileOptions::decode(&encoded).unwrap();
        assert_eq!(&decoded.backup_existing.unwrap(), "_moo");
        assert_eq!(decoded.chunk_size.unwrap(), 123);
    }
}
