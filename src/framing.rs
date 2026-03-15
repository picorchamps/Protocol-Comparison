//! Length-prefix framing helpers shared by the TCP and QUIC scenarios.
//!
//! Wire format: `[u32 LE length][payload bytes]`
//! For throughput the "big" transfer uses a `u64 LE` length prefix instead.

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Write `data` with a 4-byte LE length prefix.
pub async fn write_framed<W: AsyncWriteExt + Unpin>(w: &mut W, data: &[u8]) -> Result<()> {
    w.write_u32_le(data.len() as u32).await?;
    w.write_all(data).await?;
    Ok(())
}

/// Read a 4-byte LE length prefix then exactly that many bytes.
pub async fn read_framed<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<Vec<u8>> {
    let len = r.read_u32_le().await? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write raw data preceded by an 8-byte LE u64 total-size header.
/// Used for the 1 GiB throughput transfer where 4 bytes would overflow.
pub async fn write_large<W: AsyncWriteExt + Unpin>(w: &mut W, data: &[u8]) -> Result<()> {
    w.write_u64_le(data.len() as u64).await?;
    w.write_all(data).await?;
    Ok(())
}

/// Corresponding reader for `write_large`.
pub async fn read_large<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<Vec<u8>> {
    let len = r.read_u64_le().await? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// 4-byte sync magic sent at the start of every TCP/QUIC connection.
pub const SYNC_MAGIC: &[u8; 4] = b"PICO";

pub async fn send_sync<W: AsyncWriteExt + Unpin>(w: &mut W) -> Result<()> {
    w.write_all(SYNC_MAGIC).await?;
    Ok(())
}

pub async fn recv_sync<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<()> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).await?;
    anyhow::ensure!(&buf == SYNC_MAGIC, "sync magic mismatch");
    Ok(())
}
