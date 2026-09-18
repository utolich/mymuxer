use crate::config;
use crate::workers::{InputStats, Helper};
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

const READ_CHUNK_SIZE: usize = 7 * 188;

pub async fn remote(
    tx: broadcast::Sender<Bytes>,
    config: config::Stream,
    cancel: CancellationToken,
    mut helper: Helper,
    stats: Arc<InputStats>,
) -> Result<()> {
    let url = &config.remote_url;
    let worker_id = &config.id;
    helper.set_namespace(&format!("Input unix #{}", worker_id));
    helper.log_play(&format!("Input unix {} running", &url));

    let mut stream = UnixStream::connect(&url).await?;

    let mut timer_stats = tokio::time::interval(Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut sent_bytes: usize = 0;

    let mut last_chunk_at = Instant::now();
    let input_timeout = Duration::from_secs(5);
    let mut buffer = [0u8; READ_CHUNK_SIZE];
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                helper.log_pause("Input unix stopped");
                break;
            },
            res = timeout(input_timeout, stream.read(&mut buffer)) => {
                match res {
                    Ok(Ok(0)) => {
                        helper.log_warn("Input unix stdout closed");
                        break;
                    },
                    Ok(Ok(n)) => {
                        sent_bytes += n;
                        let _ = tx.send(Bytes::copy_from_slice(&buffer[..n]));
                        last_chunk_at = Instant::now();
                    },
                    Ok(Err(e)) => {
                        let msg = format!("Failed to fetch remote data {:?}", e);
                        helper.log_warn(&msg);
                        break;
                    },
                    Err(_) => {
                        let idle_ms = Instant::now().duration_since(last_chunk_at).as_millis();
                        helper.log_warn(&format!("Input timeout: no data for {} ms, reconnecting", idle_ms));
                        break;
                    }
                };
            },
            _ = timer_stats.tick() => {
                let bitrate: u64 = (sent_bytes * 8) as u64;
                stats.update_bitrate(bitrate);
                sent_bytes = 0;
            }
        }
    }

    Ok(())
}