#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

pub mod server;

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use hyperblobs::BlobId;
use hypercore::{Core, CoreError, GetOptions, Session};

pub use hyperblobs;

type Range = Pin<Box<dyn Future<Output = Result<bool, CoreError>> + Send>>;

#[derive(Clone, Debug)]
pub struct StreamOptions {
    /// First byte, relative to the blob.
    pub start: u64,
    /// Bytes to read; `None`: to the end of the blob.
    pub length: Option<u64>,
    /// Bytes to keep requested ahead of the reader; 0 turns prefetch off.
    /// Writable cores never prefetch.
    pub window_bytes: u64,
    /// Whether the stream's session counts as an active session.
    pub active: bool,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            start: 0,
            length: None,
            window_bytes: 0,
            active: true,
        }
    }
}

struct Window {
    start_block: u64,
    end_block: u64,
    blocks: u64,
    low_water: u64,
    dl_end: u64,
    range: Option<Range>,
}

/// Yields the requested bytes as zero-copy slices of the core's blocks.
pub struct ByteStream {
    session: Session,
    index: u64,
    relative: usize,
    remaining: u64,
    block_end: u64,
    read: u64,
    window: Option<Window>,
}

impl ByteStream {
    pub async fn open(core: &Core, id: BlobId, opts: StreamOptions) -> Result<Self, CoreError> {
        let session = core.session(opts.active);
        let start = opts.start;
        let remaining = id
            .byte_length
            .saturating_sub(start)
            .min(opts.length.unwrap_or(u64::MAX));
        let mut s = Self {
            session,
            index: id.block_offset,
            relative: 0,
            remaining,
            block_end: id.block_end(),
            read: 0,
            window: None,
        };
        if remaining == 0 {
            return Ok(s);
        }
        if start > 0 {
            let (index, relative) = s
                .session
                .seek(id.byte_offset + start, true)
                .await?
                .ok_or(CoreError::NotFound)?;
            if index < id.block_offset || index >= s.block_end {
                s.remaining = 0;
                return Ok(s);
            }
            s.index = index;
            s.relative = relative as usize;
        }
        if opts.window_bytes > 0 && !s.session.writable() {
            let end_block = if start + remaining >= id.byte_length {
                s.block_end
            } else {
                let last = id.byte_offset + start + remaining - 1;
                match s.session.seek(last, true).await? {
                    Some((i, _)) => (i + 1).min(s.block_end),
                    None => s.block_end,
                }
            };
            // Same sizing as PearTube's blob server: blocks of the blob's mean size, at least 4 KiB.
            let mean = id.byte_length.div_ceil(id.block_length.max(1)).max(4096);
            let blocks = opts.window_bytes.div_ceil(mean).max(16);
            s.window = Some(Window {
                start_block: s.index,
                end_block,
                blocks,
                low_water: 8.max(blocks - 8.max(blocks >> 2)),
                dl_end: s.index,
                range: None,
            });
            s.advance_window();
        }
        Ok(s)
    }

    /// Bytes not yet yielded.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// The `[start, end)` block range currently requested from peers.
    pub fn window(&self) -> Option<(u64, u64)> {
        let w = self.window.as_ref()?;
        w.range.as_ref()?;
        Some((w.start_block + self.read, w.dl_end))
    }

    fn advance_window(&mut self) {
        let Some(w) = self.window.as_mut() else {
            return;
        };
        let cur = w.end_block.min(w.start_block + self.read);
        if w.range.is_some() && (w.dl_end >= w.end_block || w.dl_end - cur > w.low_water) {
            return;
        }
        let next = w.end_block.min((cur + 1).max(cur + w.blocks));
        if next <= cur {
            return;
        }
        // Register the overlapping next window before the old one is cancelled.
        let prev = w
            .range
            .replace(Box::pin(self.session.download(cur, Some(next), true)));
        w.dl_end = next;
        drop(prev);
    }

    /// The next chunk, or `None` once the range is done.
    pub async fn next(&mut self) -> Result<Option<Bytes>, CoreError> {
        if self.remaining == 0 || self.index >= self.block_end {
            self.window = None;
            return Ok(None);
        }
        self.advance_window();
        let mut block = self
            .session
            .get(self.index, GetOptions::default())
            .await?
            .ok_or(CoreError::NotFound)?;
        if self.relative > 0 {
            block = block.slice(self.relative.min(block.len())..);
            self.relative = 0;
        }
        if (block.len() as u64) > self.remaining {
            block.truncate(self.remaining as usize);
        }
        self.index += 1;
        self.read += 1;
        self.remaining -= block.len() as u64;
        Ok(Some(block))
    }
}
