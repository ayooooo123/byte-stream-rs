# byte-stream-rs

Byte-range reads of [hyperblobs-rs](../hyperblobs-rs) blobs, like npm
[hypercore-byte-stream](https://github.com/holepunchto/hypercore-byte-stream), and an HTTP Range
server that is link- and response-compatible with PearTube's `src/blob-server.js`.

```rust,no_run
use byte_stream::{ByteStream, StreamOptions};

# async fn demo(core: hypercore::Core, id: hyperblobs::BlobId) -> Result<(), hypercore::CoreError> {
let opts = StreamOptions { start: 1 << 20, length: Some(4096), window_bytes: 96 << 20, ..Default::default() };
let mut s = ByteStream::open(&core, id, opts).await?;
while let Some(chunk) = s.next().await? {
    // zero-copy slices of the core's blocks
}
# Ok(()) }
```

- **Sliding window**: with `window_bytes > 0` (and a core that isn't writable), one download range
  covers the blocks just ahead of the reader. It refills from the reader's position when fewer
  than a quarter of its blocks remain ahead (PearTube's sizing: at least 16 blocks, at least 8
  ahead), registering the next range before the old one is cancelled. Dropping the stream cancels
  the range and closes its session.
- **Server**: `BlobServer::bind(addr, opts, resolve)` serves
  `/?key=<z32|hex>&blob=<z32 blob id>&type=<mime>&token=<t>` with 200/206/416, `HEAD`, CORS and
  `Content-Security-Policy: sandbox`, same as the JS server. Each response owns one session and
  one window. When the socket closes, hyper drops the body and with it the stream, session and
  window. `abort_streams`, `streaming_keys` and `stream_progress` match the JS server's hooks for
  cache eviction.

Not ported: the `anyPort` fallback (callers bind) and resolvers that return only a key (the
resolver hands back an open core).

## Tests

`cargo test`; `cargo test -- --include-ignored` also runs `tests/server.rs` (needs `npm install` in
`tests/js`): npm hyperblobs writes four blobs (uniform 512 KiB blocks, uneven write-stream chunks,
empty, one block), Rust replicates them over a local HyperDHT testnet (UDX), and the server must
return exact bytes for 75 random ranges plus the 206/416 edges. It then cuts the peer, clears the
blocks and aborts 80 concurrent requests, half stuck in seek and half mid-body, and checks that
no requests, streams or sessions remain.

## License

MIT OR Apache-2.0
