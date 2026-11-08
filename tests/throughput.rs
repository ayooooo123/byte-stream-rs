//! Opt-in cached HTTP benchmark, with bounded client memory and isolated storage.
use std::sync::Arc;
use std::time::Instant;

use byte_stream::server::{BlobServer, Resolve, Resolved, ServerOptions};
use hyperblobs::BlobId;
use hypercore::crypto::KeyPair;
use hypercore::storage::Store;
use hypercore::{Core, CoreOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn cpu_seconds() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-p", &std::process::id().to_string(), "-o", "time="])
        .output()
        .unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .trim()
        .split(':')
        .fold(0.0, |total, part| {
            total * 60.0 + part.parse::<f64>().unwrap()
        })
}

async fn read_http(server: &BlobServer, path: &str, buf: &mut [u8]) -> u64 {
    let mut socket = TcpStream::connect(server.local_addr()).await.unwrap();
    socket.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nRange: bytes=1-\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut input = BufReader::new(socket);
    let mut line = String::new();
    input.read_line(&mut line).await.unwrap();
    assert!(line.starts_with("HTTP/1.1 206"), "{line}");
    loop {
        line.clear();
        input.read_line(&mut line).await.unwrap();
        if line == "\r\n" {
            break;
        }
        assert!(!line.is_empty(), "missing HTTP headers");
    }
    let mut received = 0;
    loop {
        let n = input.read(buf).await.unwrap();
        if n == 0 {
            break;
        }
        received += n as u64;
    }
    received
}

#[test]
#[ignore = "cached HTTP throughput benchmark, run in release mode"]
fn cached_http_ranges() {
    let workers = std::env::var("STREAM_WORKERS")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap()
        .block_on(cached_ranges(workers));
}

async fn cached_ranges(workers: usize) {
    let tmp = tempfile::tempdir().unwrap();
    let store = Store::open(tmp.path()).unwrap();
    let core = Core::open(
        &store,
        CoreOptions {
            key_pair: Some(KeyPair::from_seed(&[0x44; 32])),
            ..Default::default()
        },
    )
    .unwrap();
    let _session = core.session(false);
    let data = vec![42u8; 512 * 1024];
    for _ in 0..8 {
        core.append(&[&data; 64]).await.unwrap();
    }
    let id = BlobId {
        block_offset: 0,
        block_length: core.length(),
        byte_offset: 0,
        byte_length: core.byte_length(),
    };
    let reader = core.clone();
    let resolve: Resolve = Arc::new(move |key| {
        let session = (key == *reader.key()).then(|| reader.session(false));
        Box::pin(async move { session.map(|session| Resolved { session }) })
    });
    let server = Arc::new(
        BlobServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            ServerOptions::default(),
            resolve,
        )
        .await
        .unwrap(),
    );
    let url = server.link(core.key(), &id, "application/octet-stream");
    let path = url
        .strip_prefix(&format!("http://{}", server.local_addr()))
        .unwrap()
        .to_owned();
    let cpu = cpu_seconds();
    eprintln!("TRANSFER pid={}", std::process::id());
    let started = Instant::now();
    let readers: usize = std::env::var("STREAM_READERS")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap();
    assert!(readers > 0);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..readers {
        let server = server.clone();
        let path = path.clone();
        tasks.spawn(async move {
            let mut buf = vec![0; 1024 * 1024];
            let mut bytes = 0;
            for _ in 0..8 {
                let received = read_http(&server, &path, &mut buf).await;
                assert_eq!(received, id.byte_length - 1);
                bytes += received;
            }
            bytes
        });
    }
    let mut bytes = 0u64;
    while let Some(result) = tasks.join_next().await {
        bytes += result.unwrap();
    }
    let seconds = started.elapsed().as_secs_f64();
    eprintln!(
        "PERF cached-http workers={workers} readers={readers} elapsed_s={seconds:.3} {:.2} MiB/s {:.2} combined CPU-s/GiB",
        bytes as f64 / 1048576.0 / seconds,
        (cpu_seconds() - cpu) / (bytes as f64 / 1073741824.0)
    );
    Arc::into_inner(server).unwrap().close().await;
}

fn node(
    args: &[&str],
) -> (
    tokio::process::Child,
    tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
) {
    use std::process::Stdio;
    let mut child = tokio::process::Command::new("node")
        .arg("dht.js")
        .args(args)
        .current_dir(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../hypercore-rs/tests/js"
        ))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let output = BufReader::new(child.stdout.take().unwrap()).lines();
    (child, output)
}

async fn line(
    out: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    prefix: &str,
) -> String {
    loop {
        let text = out.next_line().await.unwrap().expect("JS peer exited");
        assert!(!text.starts_with("error"), "{text}");
        if let Some(value) = text.strip_prefix(prefix) {
            return value.trim().to_owned();
        }
    }
}

async fn swarm(port: &str, core: Core, client: bool) -> peeroxide::SwarmHandle {
    let mut config = peeroxide::SwarmConfig::default();
    config.dht.dht.bootstrap = vec![format!("127.0.0.1:{port}")];
    config.dht.dht.host = "127.0.0.1".to_owned();
    config.dht.dht.port = 0;
    let (_task, handle, mut connections) = peeroxide::spawn(config).await.unwrap();
    let topic = *core.discovery_key();
    tokio::spawn(async move {
        while let Some(conn) = connections.recv().await {
            core.replicate(&protomux::Mux::from_secret_stream(conn.peer.stream));
        }
    });
    let mut opts = peeroxide::JoinOpts::default();
    opts.client = client;
    opts.server = !client;
    handle.join(topic, opts).await.unwrap();
    handle
}

async fn uncached_http(js: bool) {
    use std::time::Duration;
    use tokio::time::timeout;
    let tmp = tempfile::tempdir().unwrap();
    let (_testnet, mut tn) = node(&["testnet"]);
    let port = line(&mut tn, "bootstrap ").await;
    let mut js_writer = None;
    let mut rust_writer = None;
    let mut server_swarm = None;
    let key = if js {
        let writer_dir = tmp.path().join("writer");
        let (writer, mut out) = node(&[
            "write",
            &port,
            writer_dir.to_str().unwrap(),
            "512",
            "524288",
            &"11".repeat(32),
        ]);
        let key = hypercore_id_encoding::decode(&line(&mut out, "key ").await).unwrap();
        line(&mut out, "ready").await;
        js_writer = Some(writer);
        key
    } else {
        let store = Store::open(&tmp.path().join("writer")).unwrap();
        let core = Core::open(
            &store,
            CoreOptions {
                key_pair: Some(KeyPair::from_seed(&[0x55; 32])),
                ..Default::default()
            },
        )
        .unwrap();
        let session = core.session(true);
        let data = vec![42; 512 * 1024];
        for _ in 0..8 {
            core.append(&[&data; 64]).await.unwrap();
        }
        server_swarm = Some(swarm(&port, core.clone(), false).await);
        let key = *core.key();
        rust_writer = Some((core, session));
        key
    };
    let store = Store::open(&tmp.path().join("reader")).unwrap();
    let core = Core::open(
        &store,
        CoreOptions {
            key: Some(key),
            ..Default::default()
        },
    )
    .unwrap();
    let _session = core.session(true);
    let client = swarm(&port, core.clone(), true).await;
    timeout(Duration::from_secs(60), core.update(None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(core.length(), 512);
    let id = BlobId {
        block_offset: 0,
        block_length: 512,
        byte_offset: 0,
        byte_length: 256 << 20,
    };
    let reader = core.clone();
    let resolve: Resolve = Arc::new(move |key| {
        let session = (key == *reader.key()).then(|| reader.session(true));
        Box::pin(async move { session.map(|session| Resolved { session }) })
    });
    let mut options = ServerOptions::default();
    if let Ok(bytes) = std::env::var("STREAM_WINDOW_BYTES") {
        options.window_bytes = bytes.parse().unwrap();
    }
    let window_bytes = options.window_bytes;
    let server = BlobServer::bind("127.0.0.1:0".parse().unwrap(), options, resolve)
        .await
        .unwrap();
    let url = server.link(core.key(), &id, "application/octet-stream");
    let path = url
        .strip_prefix(&format!("http://{}", server.local_addr()))
        .unwrap();
    assert!(!core.has(0));
    let mut buf = vec![0; 1024 * 1024];
    eprintln!("TRANSFER pid={}", std::process::id());
    let cpu = cpu_seconds();
    let started = Instant::now();
    let received = timeout(Duration::from_secs(180), read_http(&server, path, &mut buf))
        .await
        .unwrap();
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(received, id.byte_length - 1);
    eprintln!(
        "PERF {}->rust-http window_bytes={window_bytes} {:.2} MiB/s {:.2} combined native CPU-s/GiB",
        if js { "js" } else { "rust" },
        received as f64 / 1048576.0 / seconds,
        (cpu_seconds() - cpu) * 1073741824.0 / received as f64
    );
    server.close().await;
    client.destroy().await.unwrap();
    if let Some(server) = server_swarm {
        server.destroy().await.unwrap();
    }
    drop(rust_writer);
    drop(js_writer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "uncached HTTP over local HyperDHT; needs hypercore-rs/tests/js dependencies"]
async fn rust_peer_streams_uncached_http() {
    uncached_http(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "uncached HTTP over local HyperDHT; needs hypercore-rs/tests/js dependencies"]
async fn js_peer_streams_uncached_http() {
    uncached_http(true).await;
}
