//! The prefetch window stays one window ahead of the reader and dies with
//! the stream.

use std::time::Duration;

use byte_stream::{ByteStream, StreamOptions};
use hyperblobs::Blobs;
use hypercore::crypto::KeyPair;
use hypercore::storage::Store;
use hypercore::{Core, CoreOptions};
use protomux::{Mux, StreamInfo, memory};
use tokio::time::{sleep, timeout};

const BLOCK: usize = 4096;

fn link() -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = memory::duplex(64);
    let info = |is_initiator| StreamInfo {
        is_initiator,
        handshake_hash: Some([7u8; 64]),
        remote_public_key: None,
    };
    (Mux::new(ar, aw, info(true)), Mux::new(br, bw, info(false)))
}

async fn until(what: &str, f: impl Fn() -> bool) {
    timeout(Duration::from_secs(10), async {
        while !f() {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn window_slides_with_the_reader_and_closes_with_it() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (sa, sb) = (
        Store::open(a.path()).unwrap(),
        Store::open(b.path()).unwrap(),
    );
    let writer = Core::open(
        &sa,
        CoreOptions {
            key_pair: Some(KeyPair::from_seed(&[3; 32])),
            ..Default::default()
        },
    )
    .unwrap();
    let data: Vec<u8> = (0..200 * BLOCK - 10)
        .map(|i| (i * 31 % 251) as u8)
        .collect();
    let id = Blobs::new(writer.clone(), BLOCK).put(&data).await.unwrap();
    let reader = Core::open(
        &sb,
        CoreOptions {
            key: Some(*writer.key()),
            ..Default::default()
        },
    )
    .unwrap();
    let (ma, mb) = link();
    writer.replicate(&ma);
    reader.replicate(&mb);
    {
        let _s = reader.session(true);
        assert!(
            timeout(Duration::from_secs(10), reader.update(None))
                .await
                .unwrap()
                .unwrap()
        );
    }

    let start = 50 * BLOCK as u64 + 5;
    let opts = StreamOptions {
        start,
        length: None,
        window_bytes: 16 * BLOCK as u64,
        active: true,
    };
    let mut s = ByteStream::open(&reader, id, opts).await.unwrap();
    assert_eq!(s.window(), Some((50, 66)));
    until("the first window", || reader.has_range(50, 66)).await;
    sleep(Duration::from_millis(100)).await;
    assert!(
        (66..200).all(|i| !reader.has(i)),
        "fetched past the window before any read"
    );

    let mut got = vec![];
    for _ in 0..20 {
        got.extend_from_slice(&s.next().await.unwrap().unwrap());
    }
    // 20 blocks read: the window refilled from block 70 once it fell to the low-water mark.
    let (lo, hi) = s.window().unwrap();
    assert!(lo <= 70 && hi > 66 && hi <= 70 + 16, "window {lo}..{hi}");
    until("the slid window", || reader.has_range(70, hi)).await;
    sleep(Duration::from_millis(100)).await;
    assert!(
        (hi..200).all(|i| !reader.has(i)),
        "fetched past the slid window"
    );

    while let Some(chunk) = s.next().await.unwrap() {
        got.extend_from_slice(&chunk);
    }
    assert!(got == data[start as usize..], "bytes differ");

    let mut s = ByteStream::open(
        &reader,
        id,
        StreamOptions {
            window_bytes: 1 << 20,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(reader.pending_requests().await > 0);
    let sessions = reader.session_count();
    s.next().await.unwrap();
    drop(s);
    assert_eq!(reader.pending_requests().await, 0);
    assert_eq!(reader.session_count(), sessions - 1);
}
