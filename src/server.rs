// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use arbitrator::Arbitrator;
use czmq::{ZFrame, ZMsg, ZSock, ZSys};
use error::{Error, Result};
use file::File;
use std::collections::HashMap;
use std::result::Result as StdResult;
use zdaemon::{Endpoint, Error as DError, ZMsgExtended};

pub struct Server {
    router: ZSock,
    sink: ZSock,
    files: HashMap<Vec<u8>, File>,
    arbitrator: Arbitrator,
    arbitrator_sock: ZSock,
}

impl Server {
    pub fn new(router: ZSock, upload_slots: u32) -> Result<Server> {
        // Would use RC instead of pipe, however RC !Send and Arc
        // +Sync & ZSock !Sync.
        let (s_sock, a_sock) = try!(ZSys::create_pipe());
        let arbitrator = try!(Arbitrator::new(a_sock, upload_slots));

        Ok(Server {
            router: router,
            sink: try!(ZSock::new_pull("inproc://zfilexfer_sink")),
            files: HashMap::new(),
            arbitrator: arbitrator,
            arbitrator_sock: s_sock,
        })
    }

    fn reply_err(&self, router_id: &[u8], err: Error) -> StdResult<(), DError> {
        let msg = try!(ZMsg::new_err(&err.into()));
        try!(msg.pushbytes(router_id));
        try!(msg.send(&self.router));
        Ok(())
    }
}

impl Endpoint for Server {
    fn get_sockets(&self) -> Vec<&ZSock> {
        vec![&self.router, &self.sink, &self.arbitrator_sock]
    }

    fn recv(&mut self, sock: &ZSock) -> StdResult<(), DError> {
        // We always expect a router ID as it ties a request to a
        // file. Its presence is not dependent on the socket type.
        let router_id = match try!(try!(ZFrame::recv(sock)).data()) {
            Ok(s) => s.into_bytes(),
            Err(b) => b,
        };

        if *sock == self.router {
            if let Ok(action) = try!(try!(ZFrame::recv(sock)).data()) {
                match action.as_ref() {
                    "NEW" => {
                        let msg = try!(ZMsg::expect_recv(sock, 5, Some(5), false));

                        let path = match msg.popstr().unwrap() {
                            Ok(p) => p,
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let size = match msg.popstr().unwrap() {
                            Ok(s) => match s.parse::<u64>() {
                                Ok(u) => u,
                                Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                            },
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let crc = match msg.popstr().unwrap() {
                            Ok(s) => match s.parse::<u64>() {
                                Ok(u) => u,
                                Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                            },
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let chunk_size = match msg.popstr().unwrap() {
                            Ok(s) => match s.parse::<u64>() {
                                Ok(u) => u,
                                Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                            },
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let options = match msg.popstr().unwrap() {
                            Ok(s) => s,
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let file = match File::create(&mut self.arbitrator, &router_id, &path, size, crc, chunk_size, &options) {
                            Ok(f) => f,
                            Err(e) => return self.reply_err(&router_id, e),
                        };

                        self.files.insert(router_id, file);
                    },
                    "CHUNK" => {
                        if !self.files.contains_key(&router_id) {
                            return self.reply_err(&router_id, Error::InvalidRequest);
                        }

                        let msg = try!(ZMsg::expect_recv(sock, 2, Some(2), false));

                        let index = match msg.popstr().unwrap() {
                            Ok(s) => match s.parse::<u64>() {
                                Ok(u) => u,
                                Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                            },
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let chunk = try!(msg.popbytes()).unwrap();

                        if let Err(e) = self.files.get_mut(&router_id).unwrap().recv(&router_id, index, chunk) {
                            return self.reply_err(&router_id, e);
                        }
                    },
                    _ => return Err(Error::InvalidRequest.into()),
                }
            }
        }
        else if *sock == self.sink {
            if !self.files.contains_key(&router_id) {
                return Err(Error::InvalidRequest.into());
            }

            let msg = try!(ZMsg::expect_recv(sock, 2, Some(2), false));

            // We can make the assumption here that the data is well
            // formed, as there are no user-provided fields.
            let index = msg.popstr().unwrap().unwrap().parse::<u64>().unwrap();
            let success = if msg.popstr().unwrap().unwrap() == "1" { true } else { false };

            let mut file = self.files.get_mut(&router_id).unwrap();

            if let Err(e) = file.sink(&mut self.arbitrator, &router_id, index, success) {
                return Err(e.into());
            }

            if file.is_error() {
                try!(ZMsg::new_err(&Error::FileFail.into()));
                try!(msg.pushbytes(&router_id));
                try!(msg.send(&self.router));
            }
            else if file.is_complete() {
                let msg = match file.save() {
                    Ok(_) => try!(ZMsg::new_ok()),
                    Err(e) => try!(ZMsg::new_err(&e.into())),
                };
                try!(msg.pushbytes(&router_id));
                try!(msg.send(&self.router));
            }
        }
        else if *sock == self.arbitrator_sock {
            // Forward messages from Arbitrator to Router sock
            let msg = try!(ZMsg::recv(sock));
            try!(msg.pushbytes(&router_id));
            try!(msg.send(&self.router));
        } else {
            unreachable!();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arbitrator::Arbitrator;
    use czmq::{RawInterface, ZFrame, ZMsg, ZSock, ZSockType, ZSys};
    use error::Error;
    use file::File;
    use std::collections::HashMap;
    use super::*;
    use tempdir::TempDir;
    use zdaemon::Endpoint;

    #[test]
    fn test_new() {
        ZSys::init();

        let router = ZSock::new(ZSockType::ROUTER);
        assert!(Server::new(router, 0).is_ok());
    }

    #[test]
    fn test_reply_err() {
        ZSys::init();

        let dealer = ZSock::new_dealer("inproc://server_test_reply_err").unwrap();
        dealer.set_sndtimeo(Some(500));
        dealer.set_rcvtimeo(Some(500));
        let router = ZSock::new_router("inproc://server_test_reply_err").unwrap();
        router.set_sndtimeo(Some(500));
        router.set_rcvtimeo(Some(500));

        dealer.send_str("moo").unwrap();
        let router_id = match ZFrame::recv(&router).unwrap().data().unwrap() {
            Ok(s) => s.into_bytes(),
            Err(b) => b,
        };
        router.flush();

        let server = new_server(router, true);
        assert!(server.reply_err(&router_id, Error::InvalidRequest).is_ok());

        let reply = ZFrame::recv(&dealer).unwrap().data().unwrap().unwrap();
        assert_eq!(&reply, "Err");
    }

    #[test]
    fn test_recv_new() {
        ZSys::init();

        let dealer = ZSock::new_dealer("inproc://server_test_recv_new").unwrap();
        dealer.set_sndtimeo(Some(500));
        dealer.set_rcvtimeo(Some(500));
        let router = ZSock::new_router("inproc://server_test_recv_new").unwrap();
        router.set_sndtimeo(Some(500));
        router.set_rcvtimeo(Some(500));
        let router_dup = ZSock::from_raw(router.borrow_raw(), false);

        let mut server = new_server(router, true);

        let msg = ZMsg::new();
        msg.addstr("NEW").unwrap();
        msg.addstr("/path/to/file").unwrap();
        msg.addstr("abc").unwrap();
        msg.addstr("0").unwrap();
        msg.addstr("1").unwrap();
        msg.addstr("{}").unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();
        assert_eq!(server.files.len(), 0);

        let msg = ZMsg::recv(&dealer).unwrap();
        assert_eq!(msg.popstr().unwrap().unwrap(), "Err");
        assert_eq!(msg.popstr().unwrap().unwrap(), "Invalid request");

        let tempdir = TempDir::new("server_test_recv_new").unwrap();

        let msg = ZMsg::new();
        msg.addstr("NEW").unwrap();
        msg.addstr(&format!("{}/testfile", tempdir.path().to_str().unwrap())).unwrap();
        msg.addstr("10240").unwrap();
        msg.addstr("0").unwrap();
        msg.addstr("1024").unwrap();
        msg.addstr("{}").unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();
        assert_eq!(server.files.len(), 1);

        assert!(dealer.recv_str().is_err());
    }

    #[test]
    fn test_recv_chunk() {
        ZSys::init();

        let dealer = ZSock::new_dealer("inproc://server_test_recv_chunk").unwrap();
        dealer.set_sndtimeo(Some(500));
        dealer.set_rcvtimeo(Some(500));
        let router = ZSock::new_router("inproc://server_test_recv_chunk").unwrap();
        router.set_sndtimeo(Some(500));
        router.set_rcvtimeo(Some(500));
        let router_dup = ZSock::from_raw(router.borrow_raw(), false);

        dealer.send_str("test").unwrap();
        let router_id = match ZFrame::recv(&router).unwrap().data().unwrap() {
            Ok(s) => s.into_bytes(),
            Err(b) => b,
        };
        router.flush();

        let mut server = new_server(router, true);

        let msg = ZMsg::new();
        msg.addstr("CHUNK").unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();

        let msg = ZMsg::recv(&dealer).unwrap();
        assert_eq!(msg.popstr().unwrap().unwrap(), "Err");
        assert_eq!(msg.popstr().unwrap().unwrap(), "Invalid request");

        let tempdir = TempDir::new("server_test_recv_chunk").unwrap();
        let file = File::create(&mut server.arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 0, 0, 1, "{}").unwrap();
        server.files.insert(router_id, file);

        let msg = ZMsg::new();
        msg.addstr("CHUNK").unwrap();
        msg.addstr("1").unwrap();
        msg.addbytes("bytes".as_bytes()).unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();

        let msg = ZMsg::recv(&dealer).unwrap();
        assert_eq!(msg.popstr().unwrap().unwrap(), "Err");
        assert_eq!(msg.popstr().unwrap().unwrap(), "Chunk index not in file");
    }

    #[test]
    fn test_recv_sink() {
        ZSys::init();

        let worker = ZSock::new_push("inproc://server_test_recv_sink").unwrap();
        let sink = ZSock::new_pull("inproc://server_test_recv_sink").unwrap();
        let sink_dup = ZSock::from_raw(sink.borrow_raw(), false);

        let mut server = new_server(sink, false);
        let tempdir = TempDir::new("server_test_recv_chunk").unwrap();
        let file = File::create(&mut server.arbitrator, "abc".as_bytes(), &format!("{}/testfile", tempdir.path().to_str().unwrap()), 1, 0, 1, "{}").unwrap();
        server.files.insert("abc".as_bytes().into(), file);

        let msg = ZMsg::new();
        msg.addstr("abc").unwrap();
        msg.addstr("0").unwrap();
        msg.addstr("1").unwrap();
        msg.send(&worker).unwrap();

        assert!(server.recv(&sink_dup).is_ok());
    }

    fn new_server(sock: ZSock, is_router: bool) -> Server {
        let router;
        let sink;
        if is_router {
            router = sock;
            sink = ZSock::new(ZSockType::PULL);
        } else {
            router = ZSock::new(ZSockType::ROUTER);
            sink = sock;
        }

        let (s_sock, a_sock) = ZSys::create_pipe().unwrap();
        let arbitrator = Arbitrator::new(a_sock, 0).unwrap();

        Server {
            router: router,
            sink: sink,
            files: HashMap::new(),
            arbitrator: arbitrator,
            arbitrator_sock: s_sock,
        }
    }
}
