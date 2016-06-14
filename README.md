# ZFileXfer [![Build Status](https://travis-ci.org/betweenlines/zfilexfer.svg?branch=master)](https://travis-ci.org/betweenlines/zfilexfer) [![Coverage Status](https://coveralls.io/repos/github/betweenlines/zfilexfer/badge.svg?branch=master)](https://coveralls.io/github/betweenlines/zfilexfer?branch=master)

ZFileXfer provides an API for sending files from a client to a server, via ZeroMQ sockets.

This project is a work in progress.

### What about FileMQ?

FileMQ has slightly different goals to ZFileXfer (catchy name, huh?). FileMQ publishes a set of files from a server to multiple clients (ala Dropbox, iCloud Docs et. al.). ZFileXfer (seriously though, I'll change the name) focuses on distributing files from disparate clients to a central server.
