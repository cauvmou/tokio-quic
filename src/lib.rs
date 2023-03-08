use std::{io, sync::Arc};

use backend::{manager::{Manager, self}, timer::Timer, server, client};
use connection::{QuicConnection, Server, Client};
use quiche::ConnectionId;
use rand::Rng;
use ring::rand::SystemRandom;
use tokio::{net::{ToSocketAddrs, UdpSocket}, sync::mpsc::{self, UnboundedReceiver}, task::JoinHandle};

mod backend;
mod crypto;
mod stream;
mod connection;
mod proto;

pub const MAX_DATAGRAM_SIZE: usize = 1350;
pub const STREAM_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug)]
pub enum Message {
    Data {
        stream_id: u64,
        bytes: Vec<u8>,
        fin: bool,
    },
    Close(u64),
}

fn default_config() -> quiche::Config {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    config.set_application_protos(quiche::h3::APPLICATION_PROTOCOL).unwrap();
    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(1_000_000);
    config.set_initial_max_stream_data_bidi_remote(1_000_000);
    config.set_initial_max_stream_data_uni(1_000_000);
    config.set_initial_max_streams_bidi(100);
    config.set_initial_max_streams_uni(100);
    config.set_disable_active_migration(true);
    config.load_priv_key_from_pem_file("./localhost.key").unwrap();
    config.load_cert_chain_from_pem_file("./localhost.crt").unwrap();
    config
}

pub struct QuicListener {
    io: Arc<UdpSocket>,
    handle: JoinHandle<Result<(), io::Error>>,
    connection_recv: UnboundedReceiver<manager::Client>,
}

impl QuicListener {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self, io::Error> {
        let io = Arc::new(UdpSocket::bind(addr).await?);
        let config = default_config();
        let rng = SystemRandom::new();
        let (tx, connection_recv) = mpsc::unbounded_channel();
        let manager = Manager::new(
            io.clone(),
            ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap(),
            b"This is a super secret token!".to_vec(),
            config,
            tx
        );
        let handle = tokio::spawn(manager);
        Ok(Self {
            io,
            handle,
            connection_recv,
        })
    }

    pub async fn accept(&mut self) -> Result<QuicConnection<Server>, io::Error> {
        let manager::Client { connection, recv } = self.connection_recv.recv().await.unwrap();

        let mut inner = server::Inner::new(
            self.io.clone(),
            connection,
            recv,
            false,
            0,
            0,
            vec![0; STREAM_BUFFER_SIZE],
            vec![0; MAX_DATAGRAM_SIZE],
            Timer::Unset,
        );

        server::Handshaker(&mut inner).await?;

        Ok(QuicConnection::<Server>::new(inner))
    }
}

pub struct QuicSocket {
    io: Arc<UdpSocket>,
    config: quiche::Config,
}

impl QuicSocket {
    pub async fn bind<A: ToSocketAddrs>(addr: A) -> Result<Self, io::Error> {
        let io = Arc::new(UdpSocket::bind(addr).await?);
        let config = default_config();
        
        Ok(Self {
            io,
            config
        })
    }

    pub async fn connect<A: ToSocketAddrs>(
        &mut self,
        server_name: Option<&str>,
        addr: A,
    ) -> Result<QuicConnection<Client>, io::Error> {
        self.io.connect(addr).await?;
        let mut scid = vec![0; 16];
        rand::thread_rng().fill(&mut *scid);
        let scid: ConnectionId = scid.into();
        let connection = quiche::connect(
            server_name,
            &scid,
            self.io.local_addr()?,
            self.io.peer_addr()?,
            &mut self.config,
        ).unwrap();

        let mut inner = client::Inner::new(
            self.io.clone(),
            connection,
            false,
            0,
            0,
            vec![0; STREAM_BUFFER_SIZE],
            vec![0; MAX_DATAGRAM_SIZE],
            Timer::Unset,
        );

        client::Handshaker(&mut inner).await?;

        Ok(QuicConnection::<Client>::new(inner))
    }
}