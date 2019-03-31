mod common;

use std::{ io, mem };
use std::time::Instant;
use std::sync::{ Arc, Mutex };
use std::collections::HashMap;
use futures::{ try_ready, Future, Stream, Poll, Async };
use tokio_timer::Delay;
use tokio_sync::{ mpsc, oneshot };
use common::{ LossyIo, to_io_error };


pub struct QuicConnector {
    config: Arc<Mutex<quiche::Config>>
}

pub struct Connecting<IO> {
    inner: MidHandshake<IO>
}

enum MidHandshake<IO> {
    Handshaking(Inner<IO>),
    End
}

pub struct Connection {
    anchor: Arc<Anchor>,
    trace_id: String,
    alpn: Vec<u8>,
    is_resumed: bool
}

pub struct Incoming {
    anchor: Arc<Anchor>,
    rx: mpsc::UnboundedReceiver<QuicStream>
}

struct Anchor(Option<oneshot::Sender<()>>);

impl Drop for Anchor {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

pub struct Driver<IO> {
    inner: Inner<IO>,
    close_recv: oneshot::Receiver<()>,
    incoming_send: mpsc::UnboundedSender<QuicStream>,
    stream_map: HashMap<u64, (mpsc::UnboundedSender<Message>, mpsc::UnboundedReceiver<Message>)>
}

pub struct QuicStream {
    id: u64,
    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Message>
}

impl Drop for QuicStream {
    fn drop(&mut self) {
        let _ = self.tx.try_send(Message::Close);
    }
}

enum Message {
    Bytes(Vec<u8>),
    End(Vec<u8>),
    Close
}

struct Inner<IO> {
    io: IO,
    connect: Box<quiche::Connection>,
    timer: Option<Delay>,
    send_buf: Vec<u8>,
    send_pos: usize,
    send_end: usize,
    send_flush: bool,
    recv_buf: Vec<u8>
}

impl<IO: LossyIo> Inner<IO> {
    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        if let Some(timer) = &mut self.timer {
            if let Ok(Async::Ready(())) = timer.poll() {
                self.connect.on_timeout();
            }
        }

        match self.connect.timeout() {
            Some(timeout) => if let Some(timer) = &mut self.timer {
                timer.reset(Instant::now() + timeout);
            } else {
                self.timer = Some(Delay::new(Instant::now() + timeout));
            },
            None => self.timer = None
        }

        self.poll_recv()?;
        self.poll_send()?;

        if self.connect.is_closed() {
            // handle close

            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn poll_send(&mut self) -> Poll<(), io::Error> {
        loop {
            if self.send_flush {
                while self.send_pos != self.send_end {
                    let n = try_ready!(self.io.poll_write(&mut self.send_buf[self.send_pos..]));
                    self.send_pos += n;
                }

                self.send_pos = 0;
                self.send_end = 0;
                self.send_flush = false;
            }

            match self.connect.send(&mut self.send_buf[self.send_end..]) {
                Ok(n) => {
                    self.send_end += n;
                    self.send_flush = self.send_end == self.send_buf.len() - 1;
                },
                Err(quiche::Error::Done) => return Ok(Async::Ready(())),
                Err(quiche::Error::BufferTooShort) => {
                    self.send_flush = true;
                    continue;
                },
                Err(err) => {
                    self.connect.close(false, err.to_wire(), b"fail")
                        .map_err(to_io_error)?;
                    return Ok(Async::NotReady);
                }
            }

            let n = try_ready!(self.io.poll_write(&mut self.send_buf[self.send_pos..self.send_end]));
            self.send_pos += n;
        }
    }

    fn poll_recv(&mut self) -> Poll<(), io::Error> {
        loop {
            let n = try_ready!(self.io.poll_read(&mut self.recv_buf));

            match self.connect.recv(&mut self.recv_buf[..n]) {
                Ok(_) => (),
                Err(quiche::Error::Done) => return Ok(Async::Ready(())),
                Err(err) => {
                    // ignore some error

                    self.connect.close(false, err.to_wire(), b"fail")
                        .map_err(to_io_error)?;
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}

impl<IO: LossyIo> Future for Connecting<IO> {
    type Item = (Driver<IO>, Connection, Incoming);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let MidHandshake::Handshaking(inner) = &mut self.inner {
            try_ready!(inner.poll_send());

            while !inner.connect.is_established() {
                if inner.connect.is_closed() {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }

                try_ready!(inner.poll_complete());
            }
        }

        match mem::replace(&mut self.inner, MidHandshake::End) {
            MidHandshake::Handshaking(inner) => {
                let (anchor, close_recv) = oneshot::channel();
                let anchor = Arc::new(Anchor(Some(anchor)));
                let (incoming_send, incoming_recv) = mpsc::unbounded_channel();

                let connection = Connection {
                    anchor: Arc::clone(&anchor),
                    trace_id: inner.connect.trace_id().to_string(),
                    alpn: inner.connect.application_proto().to_vec(),
                    is_resumed: inner.connect.is_resumed()
                };

                let incoming = Incoming { anchor, rx: incoming_recv };

                let driver = Driver {
                    inner, close_recv, incoming_send,
                    stream_map: HashMap::new()
                };

                // TODO

                Ok(Async::Ready((driver, connection, incoming)))
            },
            MidHandshake::End => panic!()
        }
    }
}

impl<IO: LossyIo> Future for Driver<IO> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            self.inner.poll_complete().map_err(drop)?;

            for stream_id in self.inner.connect.readable() {
                if !self.stream_map.get(&stream_id).is_some() {
                    // TODO
                }
            }

            // TODO

            if let Async::Ready(()) = self.close_recv.poll().map_err(drop)? {
                return Ok(Async::Ready(()));
            }
        }
    }
}

impl Stream for Incoming {
    type Item = QuicStream;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll().map_err(drop)
    }
}