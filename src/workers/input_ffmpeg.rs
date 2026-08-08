use crate::config;
use crate::workers::{InputStats, Helper};
use anyhow::Result;
use bytes::Bytes;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::broadcast;
use tokio::time::{Instant, sleep, timeout};
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
    helper.set_namespace(&format!("Input ffmpeg #{}", worker_id));
    helper.log_play(&format!("Input ffmpeg {} running", url));

    let mut timer_stats = tokio::time::interval(Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut sent_bytes: usize = 0;

    'send: loop {
        let mut args = url.split_whitespace().collect::<Vec<&str>>();
        args.append(&mut vec!["-f", "mpegts", "pipe:1"]);
        println!("{:?}", args);
        let mut ffmpeg = Command::new("ffmpeg")
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let mut stdout = ffmpeg.stdout.take().unwrap();
        let stderr = ffmpeg.stderr.take().unwrap();
        let mut reader = BufReader::new(stderr).lines();

        let mut last_chunk_at = Instant::now();
        let input_timeout = Duration::from_secs(5);
        let mut buffer = [0u8; READ_CHUNK_SIZE];
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    helper.log_pause("Input ffmpeg stopped");
                    stop_ffmpeg(&mut ffmpeg, &mut helper).await?;

                    break 'send;
                },
                res = timeout(input_timeout, stdout.read(&mut buffer)) => {
                    match res {
                        Ok(Ok(0)) => {
                            helper.log_warn("Input ffmpeg stdout closed");
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
                res = reader.next_line() => {
                    match res {
                        Ok(Some(line)) => helper.log(line.as_str()),
                        _ => {}
                    }
                },
                _ = timer_stats.tick() => {
                    let bitrate: u64 = (sent_bytes * 8) as u64;
                    stats.update_bitrate(bitrate);
                    sent_bytes = 0;
                }
            }
        }
        stop_ffmpeg(&mut ffmpeg, &mut helper).await?;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break 'send,
            _ = sleep(Duration::from_secs(3)) => {}
        }
    }

    Ok(())
}

async fn stop_ffmpeg(ffmpeg: &mut Child, helper: &mut Helper) -> Result<()> {
    match ffmpeg.try_wait()? {
        Some(status) => {
            helper.log(&format!("ffmpeg exited: {}", status));
        }
        None => {
            ffmpeg.kill().await?;
            let status = ffmpeg.wait().await?;
            helper.log(&format!("ffmpeg killed: {}", status));
        }
    }

    Ok(())
}
