// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

extern crate czmq;
extern crate rustc_serialize;
extern crate tempdir;
extern crate zdaemon;
extern crate zfilexfer;

use czmq::{ZSock, ZSys};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::thread::spawn;
use tempdir::TempDir;
use zdaemon::Service;
use zfilexfer::{File, FileOptions, Server};

#[test]
fn upload() {
    ZSys::init();

    let server = ZSock::new_router("@inproc://test_upload").unwrap();
    server.set_rcvtimeo(Some(500));
    let mut client = ZSock::new_dealer(">inproc://test_upload").unwrap();
    client.set_rcvtimeo(Some(500));

    let handle = spawn(move|| {
        let mut service = Service::new().unwrap();
        service.add_endpoint(Server::new(server, 2).unwrap()).unwrap();
        let _ = service.start(false, Some(500)); // Give this a timeout so that the test can finish!
    });

    let tempdir = TempDir::new("file_test_new_recv").unwrap();
    let path = format!("{}/file.txt", tempdir.path().to_str().unwrap());
    let mut fs_file = fs::File::create(&path).unwrap();
    fs_file.write_all("abcdefghijklmnopqrstuvwxyz".as_bytes()).unwrap();

    let file = File::open(&path, Some(&[FileOptions::BackupExisting(".bk".into()), FileOptions::ChunkSize(5)])).unwrap();
    file.send(&mut client, &path).unwrap();

    assert!(fs::metadata(&path).is_ok());
    let mut buf = PathBuf::from(&path);
    buf.set_file_name("file.txt.bk");
    assert!(fs::metadata(&buf).is_ok());

    handle.join().unwrap();
}
