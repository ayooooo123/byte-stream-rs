//! HTTP Range server for blobs, link- and response-compatible with PearTube's
//! `src/blob-server.js`: `/?key=<id>&blob=<z32 blob id>&type=<mime>&token=<t>`.
//!
//! Every request owns one core session and at most one download window.
//! When the client goes away hyper drops the body, which drops the stream,
//! its session and its window, so nothing keeps downloading.
//!
//! Not ported: `anyPort` fallback (callers bind), `resolve` returning only a
//! key (the resolver hands back an opened core).

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::stream;
use http_body_util::{BodyExt, Empty, StreamBody, combinators::UnsyncBoxBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use hyperblobs::BlobId;
use hypercore::{Core, CoreError};
use hypercore_id_encoding::z32;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use crate::{ByteStream, StreamOptions};

/// PearTube's default window: 96 MiB ahead of the HTTP cursor.
pub const DEFAULT_WINDOW_BYTES: u64 = 96 * 1024 * 1024;

/// What a resolver hands back for a requested key.
pub struct Resolved {
    pub core: Core,
    /// `false`: serve without prefetching (and with a passive session).
    pub active: bool,
}

pub type ResolveFuture = Pin<Box<dyn Future<Output = Option<Resolved>> + Send>>;
pub type Resolve = Arc<dyn Fn([u8; 32]) -> ResolveFuture + Send + Sync>;

pub struct ServerOptions {
    /// Required `token=` query value; `None` serves without one.
    pub token: Option<String>,
    pub window_bytes: u64,
}

impl Default for ServerOptions {
    /// A fresh random 32-byte hex token, the JS default.
    fn default() -> Self {
        let bytes: [u8; 32] = rand::random();
        Self {
            token: Some(hex(&bytes)),
            window_bytes: DEFAULT_WINDOW_BYTES,
        }
    }
}

struct Active {
    key: [u8; 32],
    abort: Option<oneshot::Sender<()>>,
}

struct Shared {
    token: Option<String>,
    window_bytes: u64,
    resolve: Resolve,
    next: AtomicU64,
    streams: Mutex<HashMap<u64, Active>>,
    progress: Mutex<HashMap<[u8; 32], u64>>,
}

pub struct BlobServer {
    shared: Arc<Shared>,
    addr: SocketAddr,
    accept: tokio::task::JoinHandle<()>,
}

impl BlobServer {
    pub async fn bind(addr: SocketAddr, opts: ServerOptions, resolve: Resolve) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            token: opts.token,
            window_bytes: opts.window_bytes,
            resolve,
            next: AtomicU64::new(0),
            streams: Mutex::new(HashMap::new()),
            progress: Mutex::new(HashMap::new()),
        });
        let accept = tokio::spawn(accept_loop(listener, shared.clone()));
        Ok(Self {
            shared,
            addr,
            accept,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn token(&self) -> Option<&str> {
        self.shared.token.as_deref()
    }

    /// `http://host:port/?key=…&blob=…&type=…&token=…`, as `getLink` builds it.
    pub fn link(&self, key: &[u8; 32], blob: &BlobId, mime: &str) -> String {
        let tok = self
            .token()
            .map(|t| format!("&token={t}"))
            .unwrap_or_default();
        format!(
            "http://{}/?key={}&blob={}&type={}{tok}",
            self.addr,
            hypercore_id_encoding::encode(key),
            z32::encode(&compact_encoding::encode(blob)),
            percent_encode(mime),
        )
    }

    /// Responses currently streaming a body.
    pub fn active_streams(&self) -> usize {
        self.shared.streams.lock().unwrap().len()
    }

    /// Keys with a body in flight: a cache must not evict these.
    pub fn streaming_keys(&self) -> HashSet<[u8; 32]> {
        let streams = self.shared.streams.lock().unwrap();
        streams.values().map(|a| a.key).collect()
    }

    /// Farthest byte served per key.
    pub fn stream_progress(&self) -> HashMap<[u8; 32], u64> {
        self.shared.progress.lock().unwrap().clone()
    }

    /// Cuts streams (of `only`, or all but `except`) off at once, e.g.
    /// before their blocks are cleared.
    pub fn abort_streams(&self, only: Option<[u8; 32]>, except: Option<[u8; 32]>) {
        let mut streams = self.shared.streams.lock().unwrap();
        for a in streams.values_mut() {
            if only.is_some_and(|k| k != a.key) || except == Some(a.key) {
                continue;
            }
            a.abort.take();
        }
    }

    /// Stops accepting, drops every connection and stream.
    pub async fn close(self) {
        self.accept.abort();
        let _ = self.accept.await;
    }
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    // Dropped with the accept task on close, aborting every connection.
    let mut conns = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((sock, _)) = accepted else { continue };
                let _ = sock.set_nodelay(true);
                let shared = shared.clone();
                conns.spawn(async move {
                    let svc = hyper::service::service_fn(move |req| {
                        let shared = shared.clone();
                        async move { Ok::<_, Infallible>(handle(shared, req).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(sock), svc)
                        .await;
                });
            }
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
}

type Body = UnsyncBoxBody<Bytes, io::Error>;

fn empty(status: StatusCode) -> Response<Body> {
    let mut res = Response::new(Empty::new().map_err(|e| match e {}).boxed_unsync());
    *res.status_mut() = status;
    res
}

fn cors(mut res: Response<Body>) -> Response<Body> {
    let h = res.headers_mut();
    h.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    h.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Range, If-Range"),
    );
    h.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("Accept-Ranges, Content-Length, Content-Range"),
    );
    res
}

struct Query {
    key: [u8; 32],
    blob: BlobId,
    mime: String,
    token: Option<String>,
}

fn decode_query(uri: &hyper::Uri) -> Option<Query> {
    let (mut key, mut blob, mut token) = (None, None, None);
    let mut mime = "application/octet-stream".to_owned();
    for part in uri.query()?.split('&') {
        if let Some(v) = part.strip_prefix("token=") {
            token = Some(v.to_owned());
        } else if let Some(v) = part.strip_prefix("key=") {
            key = Some(hypercore_id_encoding::decode(v).ok()?);
        } else if let Some(v) = part.strip_prefix("blob=") {
            blob = Some(compact_encoding::decode::<BlobId>(z32::decode(v).ok()?).ok()?);
        } else if let Some(v) = part.strip_prefix("type=") {
            mime = percent_decode(v)?;
        }
    }
    Some(Query {
        key: key?,
        blob: blob?,
        mime,
        token,
    })
}

enum RangeSpec {
    Suffix(u64),
    From(u64, Option<u64>),
    Invalid,
}

/// Same grammar as the JS `parseRange`: one `bytes=a-b` range or nothing.
fn parse_range(raw: &str) -> Option<RangeSpec> {
    let spec = raw.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    if b.contains('-')
        || !a.bytes().all(|c| c.is_ascii_digit())
        || !b.bytes().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let num = |s: &str| s.parse::<u64>().unwrap_or(u64::MAX);
    match (a.is_empty(), b.is_empty()) {
        (true, true) => None,
        (true, false) => Some(match num(b) {
            0 => RangeSpec::Invalid,
            n => RangeSpec::Suffix(n),
        }),
        (false, _) => Some(RangeSpec::From(num(a), (!b.is_empty()).then(|| num(b)))),
    }
}

async fn handle(shared: Arc<Shared>, req: Request<Incoming>) -> Response<Body> {
    let head = match *req.method() {
        Method::OPTIONS => return cors(empty(StatusCode::NO_CONTENT)),
        Method::GET => false,
        Method::HEAD => true,
        _ => return cors(empty(StatusCode::METHOD_NOT_ALLOWED)),
    };
    let q = match decode_query(req.uri()) {
        Some(q) if shared.token.is_none() || q.token == shared.token => q,
        _ => return cors(empty(StatusCode::NOT_FOUND)),
    };
    let range = req
        .headers()
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);
    let Some(resolved) = (shared.resolve)(q.key).await else {
        return cors(empty(StatusCode::NOT_FOUND));
    };
    cors(serve(shared, q, range, head, resolved).await)
}

async fn serve(
    shared: Arc<Shared>,
    q: Query,
    range: Option<RangeSpec>,
    head: bool,
    resolved: Resolved,
) -> Response<Body> {
    let total = q.blob.byte_length;
    let unsatisfiable = || {
        let mut res = empty(StatusCode::RANGE_NOT_SATISFIABLE);
        res.headers_mut()
            .insert(header::CONTENT_RANGE, hv(format!("bytes */{total}")));
        res
    };
    let (start, length, status) = match range {
        None => (0, total, StatusCode::OK),
        Some(_) if total == 0 => return unsatisfiable(),
        Some(RangeSpec::Invalid) => return unsatisfiable(),
        Some(RangeSpec::Suffix(n)) => (
            total.saturating_sub(n),
            n.min(total),
            StatusCode::PARTIAL_CONTENT,
        ),
        Some(RangeSpec::From(s, e)) => {
            let end = e.map_or(total - 1, |e| e.min(total - 1));
            if s >= total || s > end {
                return unsatisfiable();
            }
            (s, end - s + 1, StatusCode::PARTIAL_CONTENT)
        }
    };

    let body = if head || length == 0 {
        Empty::new().map_err(|e| match e {}).boxed_unsync()
    } else {
        let window_bytes = if resolved.active && q.blob.block_length > 0 {
            shared.window_bytes
        } else {
            0
        };
        let opts = StreamOptions {
            start,
            length: Some(length),
            window_bytes,
            active: resolved.active,
        };
        match ByteStream::open(&resolved.core, q.blob, opts).await {
            Ok(s) => body(shared, q.key, start, s),
            Err(_) => return empty(StatusCode::NOT_FOUND),
        }
    };

    let mut res = Response::new(body);
    *res.status_mut() = status;
    let h = res.headers_mut();
    if status == StatusCode::PARTIAL_CONTENT {
        h.insert(
            header::CONTENT_RANGE,
            hv(format!("bytes {start}-{}/{total}", start + length - 1)),
        );
    }
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("sandbox"),
    );
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&q.mime)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    h.insert(header::CONTENT_LENGTH, hv(length.to_string()));
    res
}

fn hv(s: String) -> HeaderValue {
    HeaderValue::from_str(&s).expect("ascii header")
}

/// Unregisters the stream however the body ends: finished, aborted, or
/// dropped by hyper because the socket closed.
struct Registered {
    shared: Arc<Shared>,
    id: u64,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.shared.streams.lock().unwrap().remove(&self.id);
    }
}

struct BodyState {
    stream: ByteStream,
    aborted: oneshot::Receiver<()>,
    key: [u8; 32],
    served: u64,
    _reg: Registered,
}

fn body(shared: Arc<Shared>, key: [u8; 32], start: u64, stream: ByteStream) -> Body {
    let id = shared.next.fetch_add(1, Ordering::Relaxed);
    let (abort, aborted) = oneshot::channel();
    shared.streams.lock().unwrap().insert(
        id,
        Active {
            key,
            abort: Some(abort),
        },
    );
    let state = BodyState {
        stream,
        aborted,
        key,
        served: start,
        _reg: Registered {
            shared: shared.clone(),
            id,
        },
    };
    let frames = stream::unfold(Some(state), |state| async move {
        let mut s = state?;
        let next = tokio::select! {
            _ = &mut s.aborted => Err(io::Error::new(io::ErrorKind::ConnectionAborted, "stream aborted")),
            n = s.stream.next() => n.map_err(core_err),
        };
        match next {
            Ok(Some(chunk)) => {
                s.served += chunk.len() as u64;
                let mut progress = s._reg.shared.progress.lock().unwrap();
                let p = progress.entry(s.key).or_insert(0);
                *p = (*p).max(s.served);
                drop(progress);
                Some((Ok(Frame::data(chunk)), Some(s)))
            }
            Ok(None) => None,
            // Error out once: hyper then closes the connection.
            Err(e) => Some((Err(e), None)),
        }
    });
    StreamBody::new(frames).boxed_unsync()
}

fn core_err(e: CoreError) -> io::Error {
    io::Error::other(e.to_string())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn percent_encode(s: &str) -> String {
    // encodeURIComponent's unreserved set.
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.!~*'()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn percent_decode(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            out.push(u8::from_str_radix(s.get(i + 1..i + 3)?, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_grammar_matches_js() {
        assert!(matches!(
            parse_range("bytes=0-"),
            Some(RangeSpec::From(0, None))
        ));
        assert!(matches!(
            parse_range("bytes=5-9"),
            Some(RangeSpec::From(5, Some(9)))
        ));
        assert!(matches!(
            parse_range("bytes=-7"),
            Some(RangeSpec::Suffix(7))
        ));
        assert!(matches!(parse_range("bytes=-0"), Some(RangeSpec::Invalid)));
        assert!(parse_range("bytes=-").is_none());
        assert!(parse_range("bytes=1-2,4-5").is_none());
        assert!(parse_range("items=1-2").is_none());
        assert!(parse_range("bytes=a-2").is_none());
    }
}
