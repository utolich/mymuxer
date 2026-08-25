use crate::{config, misc};
use crate::hls_client::HlsClient;
use crate::workers::{InputStats, Helper, send_broadcast_chunks};
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub async fn remote(
    tx: broadcast::Sender<Bytes>,
    config: config::Stream,
    cancel: CancellationToken,
    mut helper: Helper,
    stats: Arc<InputStats>,
) -> Result<()> {
    let url = &config.remote_url;
    let worker_id = &config.id;
    helper.set_namespace(&format!("Input hls #{}", worker_id));
    helper.log_play(&format!("Input hls {} running", url));

    let mut timer_stats = tokio::time::interval(Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut sent_bytes: usize = 0;

    let mut err_count = 0;
    let err_timer_duration: i32 = 3;
    let err_count_per_min = 60_i32.saturating_div(err_timer_duration);

    let client = HlsClient::new()?;

    'send: loop {
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                helper.log_pause("Input hls stopped");
                break 'send;
            },
            res = client.get(url, cancel.clone()) => res
        };

        match response {
            Ok((mut receiver, client_handle)) => {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            helper.log_pause("Input hls stopped");
                            match misc::wait_and_abort(client_handle).await {
                                Ok(true) => helper.log("Input HLS stopped"),
                                Ok(false) => helper.log("Input HLS did not stop within timeout, aborting"),
                                Err(e) => helper.log(&format!("Input HLS error: {}", e)),
                            }
                            break 'send;
                        },
                        res = receiver.recv() => {
                            match res {
                                Some(chunk) => {
                                    sent_bytes += chunk.len();
                                    send_broadcast_chunks(&tx, chunk);
                                },
                                None => {
                                    let msg = "Hls client stopped";
                                    helper.log_warn(msg);
                                    match misc::wait_and_abort(client_handle).await {
                                        Ok(true) => helper.log("Input HLS stopped"),
                                        Ok(false) => helper.log("Input HLS did not stop within timeout, aborting"),
                                        Err(e) => helper.log(&format!("Input HLS error: {}", e)),
                                    }
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
                if err_count == 0 {
                    helper.log(&format!("Failed to send hls request: {:?}", e));
                } else if err_count >= err_count_per_min {
                    helper.log(&format!("Error occurred {} times per 1 min", err_count));
                    err_count = 0;
                }
                err_count += 1;
            }
        }
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                helper.log_pause("Input hls stopped");
                break 'send;
            },
            _ = sleep(Duration::from_secs(3)) => {}
        }
    }

    Ok(())
}
