//! Acceptance: the Range server over a JS-written hyperblobs core reached
//! through HyperDHT/UDX, exact bytes for random ranges, 206/416 edges, and
//! nothing left running after 80 concurrent aborted requests.

use std::collections::HashSet;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use byte_stream::server::{BlobServer, Resolve, Resolved, ServerOptions};
use hyperblobs::BlobId;
use hypercore::crypto::KeyPair;
use hypercore::storage::Store;
use hypercore::{Core, CoreOptions};
use peeroxide::{JoinOpts, SwarmConfig, spawn};
use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::rpc::DhtConfig;
use protomux::Mux;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStdout, Command};
use tokio::time::{sleep, timeout};

fn blob(b: u64, len: usize) -> Vec<u8> {
    (0..len as u64)
        .map(|j| (((j * 7 + b * 13) ^ (j >> 9)) & 0xff) as u8)
        .collect()
}

struct Writer {
    _child: Child,
    out: Lines<BufReader<ChildStdout>>,
}

impl Writer {
    fn start(dir: &Path) -> Self {
        let js = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js"));
        assert!(
            js.join("node_modules").exists(),
            "run `npm install` in {}",
            js.display()
        );
        let mut child = Command::new("node")
            .arg("blobs.js")
            .arg(dir)
            .current_dir(js)
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("node on PATH");
        let out = BufReader::new(child.stdout.take().unwrap()).lines();
        Self { _child: child, out }
    }

    async fn line(&mut self) -> String {
        let l = self.out.next_line().await.unwrap().expect("node exited");
        assert!(!l.starts_with("error"), "{l}");
        l
    }
}

struct Http {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    first_body: Duration,
}

impl Http {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

async fn http(addr: std::net::SocketAddr, method: &str, path: &str, range: Option<&str>) -> Http {
    let started = std::time::Instant::now();
    let mut s = TcpStream::connect(addr).await.unwrap();
    let range = range.map(|r| format!("Range: {r}\r\n")).unwrap_or_default();
    s.write_all(
        format!("{method} {path} HTTP/1.1\r\nHost: x\r\n{range}Connection: close\r\n\r\n")
            .as_bytes(),
    )
    .await
    .unwrap();
    let (raw, first_body) = timeout(Duration::from_secs(60), async {
        let mut input = BufReader::new(s);
        let mut raw = Vec::new();
        loop {
            assert!(
                input.read_until(b'\n', &mut raw).await.unwrap() > 0,
                "header end"
            );
            if raw.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let mut first = [0];
        let count = input.read(&mut first).await.unwrap();
        let first_body = started.elapsed();
        raw.extend_from_slice(&first[..count]);
        input.read_to_end(&mut raw).await.unwrap();
        (raw, first_body)
    })
    .await
    .expect("response");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("header end");
    let head = String::from_utf8(raw[..split].to_vec()).unwrap();
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers = lines
        .map(|l| l.split_once(": ").unwrap())
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
    Http {
        status,
        headers,
        body: raw[split + 4..].to_vec(),
        first_body,
    }
}

fn path(key: &str, z32: &str, token: &str) -> String {
    format!("/?key={key}&blob={z32}&type=video%2Fmp4&token={token}")
}

/// Deterministic pseudo-random numbers without a dependency.
struct Lcg(u64);
impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n.max(1)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and `npm install` in tests/js"]
async fn serves_js_blobs_over_udx_and_leaks_nothing_on_abort() {
    let tmp = tempfile::tempdir().unwrap();
    let mut js = Writer::start(&tmp.path().join("js"));
    let port = js
        .line()
        .await
        .strip_prefix("bootstrap ")
        .unwrap()
        .to_owned();
    let info: serde_json::Value = serde_json::from_str(&js.line().await).unwrap();
    assert_eq!(js.line().await, "ready");
    let key_z32 = info["key"].as_str().unwrap().to_owned();
    let dk: [u8; 32] = (0..32)
        .map(|i| {
            u8::from_str_radix(
                &info["discoveryKey"].as_str().unwrap()[2 * i..2 * i + 2],
                16,
            )
            .unwrap()
        })
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let blobs: Vec<(BlobId, String)> = info["blobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            let id = &b["id"];
            let n = |f: &str| id[f].as_u64().unwrap();
            let blob_id = BlobId {
                block_offset: n("blockOffset"),
                block_length: n("blockLength"),
                byte_offset: n("byteOffset"),
                byte_length: n("byteLength"),
            };
            (blob_id, b["z32"].as_str().unwrap().to_owned())
        })
        .collect();

    // Rust peer: replicate the JS core sparsely over HyperDHT/UDX.
    let store = Store::open(&tmp.path().join("rust")).unwrap();
    let key = hypercore_id_encoding::decode(&key_z32).unwrap();
    let core = Core::open(
        &store,
        CoreOptions {
            key: Some(key),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(core.discovery_key(), &dk);
    let mut dht = DhtConfig::default();
    dht.bootstrap = vec![format!("127.0.0.1:{port}")];
    dht.host = "127.0.0.1".into();
    dht.port = 0;
    let mut hyper = HyperDhtConfig::default();
    hyper.dht = dht;
    let mut config = SwarmConfig::default();
    config.dht = hyper;
    let (_task, swarm, mut conns) = spawn(config).await.unwrap();
    let replicating = core.clone();
    tokio::spawn(async move {
        while let Some(conn) = conns.recv().await {
            replicating.replicate(&Mux::from_secret_stream(conn.peer.stream));
        }
    });
    let mut client = JoinOpts::default();
    client.server = false;
    swarm.join(dk, client).await.unwrap();
    // Kept for the whole test: the last session closing would close the core.
    let session = core.session(true);
    assert!(
        timeout(Duration::from_secs(60), session.update(None))
            .await
            .expect("found the JS peer")
            .unwrap()
    );

    let resolve_core = core.clone();
    let resolve: Resolve = Arc::new(move |k| {
        let session = (k == *resolve_core.key()).then(|| resolve_core.session(true));
        Box::pin(async move { session.map(|session| Resolved { session }) })
    });
    // A small window so the sliding prefetch actually slides.
    let opts = ServerOptions {
        window_bytes: 1024 * 1024,
        ..Default::default()
    };
    let server = BlobServer::bind("127.0.0.1:0".parse().unwrap(), opts, resolve)
        .await
        .unwrap();
    let addr = server.local_addr();
    let token = server.token().unwrap().to_owned();
    assert_eq!(
        server.link(&key, &blobs[0].0, "video/mp4"),
        format!("http://{addr}{}", path(&key_z32, &blobs[0].1, &token))
    );

    let expected: Vec<Vec<u8>> = blobs
        .iter()
        .enumerate()
        .map(|(i, (id, _))| blob(i as u64, id.byte_length as usize))
        .collect();
    // A one-byte seek still needs its uncached 512 KiB block and Merkle path.
    let offset = 4 * 512 * 1024 + 123;
    assert!(!core.has(4));
    let seek = http(
        addr,
        "GET",
        &path(&key_z32, &blobs[0].1, &token),
        Some(&format!("bytes={offset}-{offset}")),
    )
    .await;
    assert_eq!(seek.status, 206);
    assert_eq!(seek.body, expected[0][offset..=offset]);
    eprintln!(
        "PERF uncached-seek {:.3} ms",
        seek.first_body.as_secs_f64() * 1000.0
    );
    let mut rng = Lcg(42);
    for ((id, z), want) in blobs.iter().zip(&expected) {
        let p = path(&key_z32, z, &token);
        let total = id.byte_length;
        if total == 0 {
            let r = http(addr, "GET", &p, None).await;
            assert_eq!((r.status, r.body.len()), (200, 0));
            let r = http(addr, "GET", &p, Some("bytes=0-")).await;
            assert_eq!(
                (r.status, r.header("content-range")),
                (416, Some("bytes */0"))
            );
            continue;
        }
        for _ in 0..25 {
            let a = rng.below(total);
            let (range, start, end) = match rng.below(4) {
                0 => (format!("bytes={a}-"), a, total - 1),
                1 => {
                    let n = 1 + rng.below(total);
                    (format!("bytes=-{n}"), total - n, total - 1)
                }
                _ => {
                    let b = a + rng.below(total - a + 1000);
                    (format!("bytes={a}-{b}"), a, b.min(total - 1))
                }
            };
            let r = http(addr, "GET", &p, Some(&range)).await;
            assert_eq!(r.status, 206, "{range}");
            assert_eq!(
                r.header("content-range"),
                Some(format!("bytes {start}-{end}/{total}").as_str())
            );
            assert_eq!(
                r.header("content-length"),
                Some((end - start + 1).to_string().as_str())
            );
            assert!(
                r.body == want[start as usize..=end as usize],
                "bytes differ for {range}"
            );
        }
        let full = http(addr, "GET", &p, None).await;
        assert_eq!(
            (full.status, full.header("accept-ranges")),
            (200, Some("bytes"))
        );
        assert!(full.body == *want);
        let last = http(addr, "GET", &p, Some(&format!("bytes={}-", total - 1))).await;
        assert_eq!(
            (last.status, last.body.as_slice()),
            (206, &want[want.len() - 1..])
        );
        let past = http(addr, "GET", &p, Some(&format!("bytes={total}-"))).await;
        assert_eq!(
            (past.status, past.header("content-range")),
            (416, Some(format!("bytes */{total}").as_str()))
        );
        let backwards = http(addr, "GET", &p, Some("bytes=5-4")).await;
        assert_eq!(backwards.status, 416);
        let zero_suffix = http(addr, "GET", &p, Some("bytes=-0")).await;
        assert_eq!(zero_suffix.status, 416);
        let head = http(addr, "HEAD", &p, None).await;
        assert_eq!(
            (head.status, head.header("content-length"), head.body.len()),
            (200, Some(total.to_string().as_str()), 0)
        );
    }
    let p0 = path(&key_z32, &blobs[0].1, &token);
    assert_eq!(
        http(addr, "GET", &p0.replace(&token, "wrong"), None)
            .await
            .status,
        404
    );
    assert_eq!(http(addr, "GET", "/?key=nope", None).await.status, 404);
    assert_eq!(http(addr, "POST", &p0, None).await.status, 405);
    let opt = http(addr, "OPTIONS", &p0, None).await;
    assert_eq!(
        (opt.status, opt.header("access-control-allow-origin")),
        (204, Some("*"))
    );
    assert!(server.stream_progress()[&key] > 0);
    assert_eq!(core.pending_requests().await, 0);

    // Hard cancel: cut the peer, drop the cached blocks, then abort 80 requests mid-flight.
    let _ = swarm.destroy().await;
    let (big, big_z) = &blobs[0];
    core.clear(big.block_offset, big.block_end()).await.unwrap();
    let p = path(&key_z32, big_z, &token);
    let baseline_sessions = core.session_count();
    let mut socks = vec![];
    for i in 0..80u64 {
        let mut s = TcpStream::connect(addr).await.unwrap();
        // Half wait inside the body on a missing block, half inside seek before headers.
        let range = if i % 2 == 0 {
            String::new()
        } else {
            format!("Range: bytes={}-\r\n", 1 + i * 4099)
        };
        s.write_all(format!("GET {p} HTTP/1.1\r\nHost: x\r\n{range}\r\n").as_bytes())
            .await
            .unwrap();
        socks.push(s);
    }
    // Every one of the 80 streams must be registered before the abort, or the
    // one still being accepted escapes it and hangs on a cleared block forever.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while server.active_streams() < 80 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "requests never reached the server"
        );
        sleep(Duration::from_millis(20)).await;
    }
    // Abort without touching the sockets: both halves must be cut off anyway,
    // the ones stuck in seek before their headers and the ones mid-body whose
    // clients stopped reading (hyper never polls those bodies again).
    server.abort_streams(None, None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let pending = core.pending_requests().await;
        let (streams, sessions) = (server.active_streams(), core.session_count());
        if pending == 0 && streams == 0 && sessions == baseline_sessions {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leaked: {pending} requests, {streams} streams, {sessions} sessions"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert!(server.streaming_keys().is_empty());
    drop(socks);
    server.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn abort_stops_only_the_given_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    let open = |seed| {
        Core::open(
            &store,
            CoreOptions {
                key_pair: Some(KeyPair::from_seed(&[seed; 32])),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let (a, b) = (open(11), open(12));
    // Kept alive: a resolver hands out one session per request.
    let (_sa, _sb) = (a.session(true), b.session(true));
    let (ka, kb) = (*a.key(), *b.key());
    let (ra, rb) = (a.clone(), b.clone());
    let resolve: Resolve = Arc::new(move |k| {
        let core = if k == ka {
            Some(ra.clone())
        } else if k == kb {
            Some(rb.clone())
        } else {
            None
        };
        Box::pin(async move {
            core.map(|core| Resolved {
                session: core.session(true),
            })
        })
    });
    let opts = ServerOptions {
        token: None,
        ..Default::default()
    };
    let server = BlobServer::bind("127.0.0.1:0".parse().unwrap(), opts, resolve)
        .await
        .unwrap();
    let addr = server.local_addr();
    let blob = BlobId {
        block_offset: 0,
        block_length: 4096,
        byte_offset: 0,
        byte_length: 1 << 20,
    };
    let z = hypercore_id_encoding::z32::encode(&compact_encoding::encode(&blob));
    // Neither core has blocks or peers: both requests wait until they are cut.
    let mut socks = vec![];
    for key in [ka, kb] {
        let p = path(&hypercore_id_encoding::encode(&key), &z, "");
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(format!("GET {p} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        socks.push(s);
    }
    // Each request: one session handed by the resolver, one for its stream.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while a.session_count() != 2 || b.session_count() != 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "requests never opened"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(server.active_streams(), 2);

    server.abort_streams(Some(ka), None);
    assert_eq!(server.streaming_keys(), HashSet::from([kb]));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let (pending, sessions) = (a.pending_requests().await, a.session_count());
        if pending == 0 && sessions == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leaked on the aborted key: {pending} requests, {sessions} sessions"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(b.session_count(), 2);
    assert!(b.pending_requests().await > 0);
    assert_eq!(server.active_streams(), 1);

    server.abort_streams(None, None);
    assert_eq!(server.active_streams(), 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        let (pending, sessions) = (b.pending_requests().await, b.session_count());
        if pending == 0 && sessions == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "leaked on the other key: {pending} requests, {sessions} sessions"
        );
        sleep(Duration::from_millis(10)).await;
    }
    drop(socks);
    server.close().await;
}
