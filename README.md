# byte-stream-rs

Byte-range reads of [hyperblobs-rs](../hyperblobs-rs) blobs, like npm
[hypercore-byte-stream](https://github.com/holepunchto/hypercore-byte-stream), and an HTTP Range
server that is link- and response-compatible with PearTube's `src/blob-server.js`.

```rust,no_run
use byte_stream::{ByteStream, StreamOptions};
use hypercore::RequestBag;

# async fn demo(core: hypercore::Core, id: hyperblobs::BlobId) -> Result<(), hypercore::CoreError> {
let opts = StreamOptions { start: 1 << 20, length: Some(4096), window_bytes: 96 << 20, ..Default::default() };
let bag = RequestBag::new();
let mut s = ByteStream::open(&core, id, opts, bag.clone()).await?;
while let Some(chunk) = s.next().await? {
    // zero-copy slices of the core's blocks
}
# Ok(()) }
```

- **Sliding window**: with `window_bytes > 0` (and a core that isn't writable), one download range
  covers the blocks just ahead of the reader. It refills from the reader's position when fewer
  than a quarter of its blocks remain ahead (PearTube's sizing: at least 16 blocks, at least 8
  ahead), registering the next range before the old one is cancelled. The stream's `RequestBag`
  cancels its block and range requests from anywhere (`bag.destroy(reason)`); dropping the stream
  only cancels them at its next poll, and closes its session.
- **Server**: `BlobServer::bind(addr, opts, resolve)` serves
  `/?key=<z32|hex>&blob=<z32 blob id>&type=<mime>&token=<t>` with 200/206/416, `HEAD`, CORS and
  `Content-Security-Policy: sandbox`, same as the JS server. A request is registered before its
  stream opens, then served by a task that owns one session and one window and feeds hyper frames
  through a bounded channel. `abort_streams` cancels a request's bag, aborts its task and drops its
  session and window at once: it does not wait for hyper to poll the body again, so a paused client
  keeps neither. When the socket closes, hyper drops the body and with it the stream.
  `abort_streams`, `streaming_keys` and `stream_progress` match the JS server's hooks for cache
  eviction.
  The body channel holds at most four blocks (2 MiB for PearTube's 512 KiB blocks);
  the 96 MiB download window writes ahead to storage, not a 96 MiB RAM queue.
  On macOS, accepted sockets inherit a 512 KiB send buffer, avoiding repeated
  partial writes for one video block while keeping the application queue bounded.

Not ported: the `anyPort` fallback (callers bind) and resolvers that return only a key (the
resolver hands back an open session).

## Tests

`cargo test`; `cargo test -- --include-ignored` also runs `tests/server.rs` (needs `npm install` in
`tests/js`): npm hyperblobs writes four blobs (uniform 512 KiB blocks, uneven write-stream chunks,
empty, one block), Rust replicates them over a local HyperDHT testnet (UDX), and the server must
return exact bytes for 75 random ranges plus the 206/416 edges. It then cuts the peer, clears the
blocks, then aborts 80 concurrent requests over sockets it never reads, half stuck in seek and
half mid-body, and checks that no requests, streams or sessions remain.

`cargo test --release --test throughput -- --ignored --nocapture` measures cached
Range serving with bounded-memory clients (256 MiB core, four concurrent readers
making eight reads each). `STREAM_WORKERS` selects runtime workers (default 4);
`STREAM_READERS` selects readers (default 4). Three-run Apple Silicon release
medians with four readers: 1 worker 1,128 MiB/s and 0.85 combined server/client
CPU-seconds/GiB; 4 workers 1,365 MiB/s and 1.61 CPU-seconds/GiB. More workers
improve this loopback workload's throughput, not its CPU efficiency; no extra
pool is needed. Whole-process peak RSS was 260–282 MiB with one worker and
230–261 MiB with four, including RocksDB, fixture creation and client buffers.
Each client allocates 1 MiB. The server channel holds four blocks, with another
block possibly waiting to enter it and one being sent; these are shared `Bytes`,
not per-chunk payload copies. The 96 MiB window is a storage request range,
not a second body buffer. No queue or window default was changed.
The JS server test also reports first-body-byte latency for a one-byte seek
into an uncached 512 KiB block.
The same benchmark target also measures a fresh 256 MiB HTTP stream from a Rust
or npm Hypercore peer over an isolated HyperDHT testnet (requires the sibling
`hypercore-rs/tests/js` dependencies). CPU includes the native HTTP client and
server; Rust-to-Rust also includes its in-process seeder.

`STREAM_WINDOW_BYTES=0` disables prefetch in the uncached benchmarks only.
For a Rust seeder and fresh Rust receiver (256 MiB, four workers), three-run
release medians were 23.63 MiB/s and 240.3 MiB process peak RSS without prefetch,
versus 33.47 MiB/s and 287.6 MiB RSS with the default 96 MiB window. Combined
native CPU was 61.32 versus 63.92 s/GiB. Disabling the window was rejected:
it cut throughput by 29%. The window is not a 96 MiB body allocation, but
RocksDB and replication buffering still increased this workload's process
peak by about 47 MiB; the process includes both Rust peers and the HTTP client.

## License

MIT OR Apache-2.0
