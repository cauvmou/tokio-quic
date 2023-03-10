# tokio-quic

Async QUIC Listener/Socket for [tokio](https://tokio.rs/) using [quiche](https://github.com/cloudflare/quiche).

## Roadmap

- [x] QuicListener for server use.
- [x] QuicSocket for client use.
- [x] Boringssl key generation (Can be disabled by disabling the default features).
- [ ] Http3 support.
- [ ] Swappable backend (quiche & quinn) for boringssl and openssl support.
- [ ] Unified config for both implementations.

## Examples

### Client

First create a `QuicSocket`.
```rs
let mut connection: QuicConnection<Client> = QuicSocket::bind("127.0.0.1:0").await?
        .connect(Some("localhost"), "127.0.0.1:4433").await?;
```
Then you can start opening new `QuicStream`s or receive incoming ones from the server.
```rs
let mut stream: QuicStream = connection.open().await;
```
```rs
let mut stream: QuicStream = connection.incoming().await.unwrap();
```
These implement the tokio `AsyncRead` and `AsyncWrite` traits.

### Server

Again create a `QuicListener`.

```rs
let mut listener: QuicListener = QuicListener::bind("127.0.0.1:4433").await?;
```
Then you can use a while loop to accept incoming connection and either handle them directly on the thread or move them to a new one.
```rs
while let Ok(mut connection: QuicConnection<Server>) = listener.accept().await {
    tokio::spawn(async move {
        ...
    });
}
```