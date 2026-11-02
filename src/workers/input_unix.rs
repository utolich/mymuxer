use crate::{config, mux};
use crate::workers::{InputStats, Helper};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout, Instant};
use tokio_util::sync::CancellationToken;

const READ_CHUNK_SIZE: usize = 7 * 188;
const IN_BUFFER_SIZE: usize = 64 * 1024;

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

    let mut timer_stats = tokio::time::interval(Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut sent_bytes: usize = 0;

    let mut err_count = 0;
    let err_timer_duration: i32 = 3;
    let err_count_per_min = 60_i32.saturating_div(err_timer_duration);

    let biss = mux::init_biss(&config.biss_key)
        .inspect_err(|e| {
            helper.clone().log(&format!("BISS init failed: {}", e));
        })
        .ok();

    let mut dirty = false;

    'send: loop {
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                helper.log_pause("Input udp stopped");
                break 'send;
            },
            res = UnixStream::connect(&url) => res
        };
        match response {
            Ok(mut res) => {
                let mut last_chunk_at = Instant::now();
                let input_timeout = Duration::from_secs(5);
                let mut buffer = [0u8; READ_CHUNK_SIZE];
                let mut in_buff = BytesMut::with_capacity(IN_BUFFER_SIZE);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            helper.log_pause("Input unix stopped");
                            break 'send;
                        },
                        res = timeout(input_timeout, res.read(&mut buffer)) => {
                            match res {
                                Ok(Ok(0)) => {
                                    dirty = true;
                                    helper.mark_dirty("Input unix stdout closed");
                                    break;
                                },
                                Ok(Ok(n)) => {
                                    let pkt = match &biss {
                                        Some(biss) => {
                                            in_buff.extend_from_slice(&buffer[..n]);
                                            biss.descramble_ts_stream(&mut in_buff)
                                                .inspect_err(|e| helper.log(&format!("BISS descramble error : {}", e)))?
                                        },
                                        None => Some(Bytes::copy_from_slice(&buffer[..n])),
                                    };

                                    if let Some(pkt) = pkt {
                                        sent_bytes += pkt.len();
                                        let _ = tx.send(pkt);
                                        last_chunk_at = Instant::now();
                                        if dirty {
                                            dirty = false;
                                            helper.cmd_unmark_dirty_now()
                                        }
                                    }
                                },
                                Ok(Err(e)) => {
                                    dirty = true;
                                    helper.mark_dirty(&format!("Failed to fetch remote data {:?}", e));
                                    break;
                                },
                                Err(_) => {
                                    dirty = true;
                                    let idle_ms = Instant::now().duration_since(last_chunk_at).as_millis();
                                    helper.mark_dirty(&format!("Input timeout: no data for {} ms, reconnecting", idle_ms));
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
            }
            Err(e) => {
                dirty = true;
                helper.mark_dirty(&format!("Connect to {} failed: {:?}", url, e));
                if err_count >= err_count_per_min {
                    helper.log(&format!("Error occurred {} times per 1 min", err_count));
                    err_count = 0;
                }
                err_count += 1;
            }
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break 'send,
            _ = sleep(Duration::from_secs(err_timer_duration as u64)) => {}
        }
    }
    Ok(())
}