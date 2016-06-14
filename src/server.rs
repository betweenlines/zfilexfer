// Copyright 2016 ZFilexfer Developers. See the COPYRIGHT file at the
// top-level directory of this distribution and at
// https://intecture.io/COPYRIGHT.
//
// Licensed under the Mozilla Public License 2.0 <LICENSE or
// https://www.tldrlegal.com/l/mpl-2.0>. This file may not be copied,
// modified, or distributed except according to those terms.

use arbitrator::Arbitrator;
use czmq::{ZFrame, ZMsg, ZSock};
use error::{Error, Result};
use file::File;
use std::collections::HashMap;
use std::result::Result as StdResult;
use std::rc::Rc;
use zdaemon::{Endpoint, Error as DError, ZMsgExtended};

pub struct Server {
    router: Rc<ZSock>,
    sink: ZSock,
    chunk_size: usize,
    files: HashMap<Vec<u8>, File>,
    arbitrator: Arbitrator,
}

impl Server {
    pub fn new(router: ZSock, chunk_size: usize) -> Result<Server> {
        let router_rc = Rc::new(router);
        let arbitrator = Arbitrator::new(router_rc.clone());

        Ok(Server {
            router: router_rc,
            sink: try!(ZSock::new_pull("inproc://zfilexfer_sink")),
            chunk_size: chunk_size,
            files: HashMap::new(),
            arbitrator: arbitrator,
        })
    }

    fn reply_err(&self, router_id: &[u8], err: Error) -> StdResult<(), DError> {
        let msg = try!(ZMsg::new_err(&err.into()));
        try!(msg.pushbytes(router_id));
        try!(msg.send(&*self.router));
        Ok(())
    }
}

impl Endpoint for Server {
    fn get_sockets(&self) -> Vec<&ZSock> {
        vec![&self.router, &self.sink]
    }

    fn recv(&mut self, sock: &ZSock) -> StdResult<(), DError> {
        // We always expect a router ID as it ties a request to a
        // file. Its presence is not dependent on the socket type.
        let router_id = match try!(try!(ZFrame::recv(sock)).data()) {
            Ok(s) => s.into_bytes(),
            Err(b) => b,
        };

        if *sock == *self.router {
            if let Ok(action) = try!(try!(ZFrame::recv(sock)).data()) {
                match action.as_ref() {
                    "NEW" => {
                        let msg = try!(ZMsg::expect_recv(sock, 2, Some(2), false));

                        let path = match msg.popstr().unwrap() {
                            Ok(p) => p,
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let size = match msg.popstr().unwrap() {
                            Ok(s) => match s.parse::<usize>() {
                                Ok(u) => u,
                                Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                            },
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let file = match File::create(&self.arbitrator, &path, size, self.chunk_size) {
                            Ok(f) => f,
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        self.files.insert(router_id, file);
                    },
                    "CHUNK" => {
                        if !self.files.contains_key(&router_id) {
                            return self.reply_err(&router_id, Error::InvalidRequest);
                        }

                        let msg = try!(ZMsg::expect_recv(sock, 2, Some(2), false));

                        let index = match msg.popstr().unwrap() {
                            Ok(s) => match s.parse::<usize>() {
                                Ok(u) => u,
                                Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                            },
                            Err(_) => return self.reply_err(&router_id, Error::InvalidRequest),
                        };

                        let chunk = match msg.popstr().unwrap() {
                            Ok(s) => s.into_bytes(),
                            Err(b) => b,
                        };

                        if let Err(e) = self.files.get(&router_id).unwrap().recv(&router_id, index, chunk) {
                            return self.reply_err(&router_id, e);
                        }
                    },
                    _ => return Err(Error::InvalidRequest.into()),
                }
            }
        }
        else if *sock == self.sink {
            if !self.files.contains_key(&router_id) {
                return self.reply_err(&router_id, Error::InvalidRequest);
            }

            let msg = try!(ZMsg::expect_recv(sock, 3, Some(3), false));

            // We can make the assumption here that the data is well
            // formed, as there are no user-provided fields.
            let index = msg.popstr().unwrap().unwrap().parse::<usize>().unwrap();
            let success = msg.popstr().unwrap().unwrap().parse::<bool>().unwrap();

            let mut file = self.files.get_mut(&router_id).unwrap();

            // Don't use reply_err() as any errors here indicate a
            // deeper problem that is unrelated to a specific file.
            if let Err(e) = file.sink(&self.arbitrator, index, success) {
                return Err(e.into());
            }

            if file.is_complete() {
                match file.save() {
                    Ok(_) => {
                        let msg = try!(ZMsg::new_ok());
                        try!(msg.pushbytes(&router_id));
                        try!(msg.send(&*self.router));
                    },
                    Err(e) => {
                        let msg = try!(ZMsg::new_err(&e.into()));
                        try!(msg.pushbytes(&router_id));
                        try!(msg.send(&*self.router));
                    },
                }
            }
        } else {
            unimplemented!();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arbitrator::Arbitrator;
    use czmq::{RawInterface, zsys_init, ZFrame, ZMsg, ZSock, ZSockType};
    use error::Error;
    use file::File;
    use std::collections::HashMap;
    use std::rc::Rc;
    use super::*;
    use zdaemon::Endpoint;

    #[test]
    fn test_new() {
        zsys_init();

        let router = ZSock::new(ZSockType::ROUTER);
        assert!(Server::new(router, 1024).is_ok());
    }

    #[test]
    fn test_reply_err() {
        zsys_init();

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

        let server = new_server(router);
        assert!(server.reply_err(&router_id, Error::InvalidRequest).is_ok());

        let reply = ZFrame::recv(&dealer).unwrap().data().unwrap().unwrap();
        assert_eq!(&reply, "Err");
    }

    #[test]
    fn test_recv_new() {
        zsys_init();

        let dealer = ZSock::new_dealer("inproc://server_test_recv_new").unwrap();
        dealer.set_sndtimeo(Some(500));
        dealer.set_rcvtimeo(Some(500));
        let router = ZSock::new_router("inproc://server_test_recv_new").unwrap();
        router.set_sndtimeo(Some(500));
        router.set_rcvtimeo(Some(500));
        let router_dup = ZSock::from_raw(router.borrow_raw(), false);

        let mut server = new_server(router);

        let msg = ZMsg::new();
        msg.addstr("NEW").unwrap();
        msg.addstr("/path/to/file").unwrap();
        msg.addstr("abc").unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();
        assert_eq!(server.files.len(), 0);

        let msg = ZMsg::recv(&dealer).unwrap();
        assert_eq!(msg.popstr().unwrap().unwrap(), "Err");
        assert_eq!(msg.popstr().unwrap().unwrap(), "Invalid request");

        let msg = ZMsg::new();
        msg.addstr("NEW").unwrap();
        msg.addstr("/path/to/file").unwrap();
        msg.addstr("10240").unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();
        assert_eq!(server.files.len(), 1);

        assert!(dealer.recv_str().is_err());
    }

    #[test]
    fn test_recv_chunk() {
        zsys_init();

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

        let mut server = new_server(router);

        let msg = ZMsg::new();
        msg.addstr("CHUNK").unwrap();
        msg.send(&dealer).unwrap();

        server.recv(&router_dup).unwrap();

        let msg = ZMsg::recv(&dealer).unwrap();
        assert_eq!(msg.popstr().unwrap().unwrap(), "Err");
        assert_eq!(msg.popstr().unwrap().unwrap(), "Invalid request");

        let file = File::create(&server.arbitrator, "/path/to/file", 0, 1).unwrap();
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

    fn new_server(router: ZSock) -> Server {
        let router_rc = Rc::new(router);
        let arbitrator = Arbitrator::new(router_rc.clone());
        Server {
            router: router_rc,
            sink: ZSock::new(ZSockType::PULL),
            chunk_size: 1024,
            files: HashMap::new(),
            arbitrator: arbitrator,
        }
    }
}
