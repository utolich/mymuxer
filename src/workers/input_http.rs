use crate::{config, mux};
use crate::workers::{InputStats, Helper, send_broadcast_chunks};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

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
    helper.set_namespace(&format!("Input http #{}", worker_id));
    helper.log_play(&format!("Input http {} running", url));

    let mut timer_stats = tokio::time::interval(Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut sent_bytes: usize = 0;

    let mut err_count = 0;
    let err_timer_duration: i32 = 3;
    let err_count_per_min = 60_i32.saturating_div(err_timer_duration);

    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::limited(5))
        .connect_timeout(Duration::from_secs(3))
        .user_agent("mymuxer")
        .build()?;

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
                helper.log_pause("Input http stopped");
                break 'send;
            },
            res = client.get(url).send() => res
        };
        match response {
            Ok(res) => {
                if !res.status().is_success() {
                    dirty = true;
                helper.mark_dirty(&format!("Status: {}", res.status()));
                    if err_count >= err_count_per_min {
                        helper.log(&format!("Error occurred {} times per 1 min", err_count));
                        err_count = 0;
                    }
                    err_count += 1;
                    continue;
                }
                let mut stream = res.bytes_stream();
                let mut last_chunk_at = Instant::now();
                let input_timeout = Duration::from_secs(5);
                let mut in_buff = BytesMut::with_capacity(IN_BUFFER_SIZE);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            helper.log_pause("Input http stopped");

                            break 'send;
                        },
                        res = timeout(input_timeout, stream.next()) => {
                            match res {
                                Ok(Some(Ok(chunk))) => {
                                    let pkt = match &biss {
                                        Some(biss) => {
                                            in_buff.extend_from_slice(&chunk);
                                            biss.descramble_ts_stream(&mut in_buff)
                                                .inspect_err(|e| helper.log(&format!("BISS descramble error : {}", e)))?
                                        },
                                        None => Some(chunk),
                                    };

                                    if let Some(pkt) = pkt {
                                        sent_bytes += send_broadcast_chunks(&tx, pkt);
                                        last_chunk_at = Instant::now();
                                        if dirty {
                                            dirty = false;
                                            helper.cmd_unmark_dirty_now()
                                        }
                                    }
                                    err_count = 0;
                                },
                                Ok(Some(Err(e))) => {
                                    dirty = true;
                                    helper.mark_dirty(&format!("Failed to fetch remote data: {:?}", e));
                                },
                                Ok(None) => {
                                    dirty = true;
                                    helper.mark_dirty("Connection closed by remote");
                                    if err_count >= err_count_per_min {
                                        helper.log(&format!("Error occurred {} times per 1 min", err_count));
                                        err_count = 0;
                                    }
                                    err_count += 1;
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
                        },
                    }
                }
            }
            Err(e) => {
                dirty = true;
                helper.mark_dirty(&format!("Failed to send HTTP request: {:?}", e));
                if err_count >= err_count_per_min {
                    helper.log(&format!("Error occurred {} times per 1 min", err_count));
                    err_count = 0;
                }
                err_count += 1;
            }
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                helper.log_pause("Input http stopped");
                break 'send;
            },
            _ = sleep(Duration::from_secs(3)) => {}
        }
    }

    Ok(())
}
