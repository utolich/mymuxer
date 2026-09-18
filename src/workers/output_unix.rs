use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::{config, misc};
use crate::mux::PacketHandler;
use crate::workers::{Helper, OutputStats};

const BROADCAST_CAPACITY: usize = 256;
pub async fn output(
    rtx: broadcast::Sender<Bytes>,
    output: config::Output,
    cancel: CancellationToken,
    mut helper: Helper,
    stats: Arc<OutputStats>,
) -> Result<()> {
    helper.set_output_id(output.id);
    helper.set_namespace(&format!("Output unix #{}", output.id));
    helper.log_play(&format!("Output unix {} running", output.output_url));

    let packet_handler_helper = helper.clone();

    if let Some(packet_handler) = PacketHandler::new(output, packet_handler_helper, stats) {
        match server(rtx, packet_handler, cancel).await {
            Ok(_) => {}
            Err(e) => {
                helper.log_warn(&format!("Error in output worker: {:?}", e));
            }
        }
    }

    Ok(())
}

async fn server(
    rtx: broadcast::Sender<Bytes>,
    mut packet_handler: PacketHandler,
    cancel: CancellationToken,
) -> Result<()> {
    let socket_path = packet_handler.output.output_url.clone();
    if let Ok(exists) = fs::try_exists(&socket_path).await {
        if exists && let Err(e) = fs::remove_file(&socket_path).await {
            return Err(anyhow!("Error removing socket file: {:?}", e));
        }
    }
    let listener = UnixListener::bind(&socket_path)?;

    let (tx, _rx) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);

    let mut rrx = rtx.subscribe();

    let cancel_accept = cancel.clone();
    let tx_accept = tx.clone();
    let mut helper_accept = packet_handler.helper.clone();
    let accept_task = tokio::spawn(async move {
        let mut err_count = 0;
        loop {
            tokio::select! {
                biased;
                _ = cancel_accept.cancelled() => {
                    helper_accept.log_pause("Output unix listen stoping");
                    break;
                },
                res = listener.accept() => {
                    match res {
                        Ok((mut stream, _addr)) => {
                            err_count = 0;
                            let cancel_clone = cancel_accept.clone();
                            let tx_clone = tx_accept.clone();
                            let mut helper_clone = helper_accept.clone();
                            tokio::spawn(async move {
                                let mut rx = tx_clone.subscribe();
                                loop {
                                    tokio::select! {
                                        biased;
                                        _ = cancel_clone.cancelled() => {
                                            break;
                                        },
                                        res = rx.recv() => {
                                            match res {
                                                Ok(chunk) => {
                                                    if let Err(e) = stream.write_all(&chunk).await {
                                                        helper_clone.log(&format!("Client disconnected: {:?}", e));
                                                        break;
                                                    }
                                                },
                                                Err(broadcast::error::RecvError::Lagged(n)) => {
                                                    helper_clone.log(&format!("lagged {}", n));
                                                    continue;
                                                },
                                                Err(e) => {
                                                    helper_clone.log(&format!("received failed:{:?}", e));
                                                    continue;
                                                }
                                            };
                                        },
                                    }
                                }
                            });
                        },
                        Err(e) => {
                            if err_count == 0 {
                                helper_accept.log_warn(&format!("Accept new connection error: {:?}", e));
                            } else {
                                helper_accept.cmd_mark_dirty();
                                break;
                            }
                            err_count += 1;
                        }
                    }
                }
            }
        }
    });


    let mut timer_stats = tokio::time::interval(std::time::Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut sent_bytes: usize = 0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                packet_handler.helper.log_pause("Output unix stoping");
                break;
            },
            res = rrx.recv() => {
                match res {
                    Ok(chunk) => {
                        if packet_handler.output.proxy {
                            sent_bytes += packet_handler.proxy_packet(chunk, |pkt| async {
                                match tx.send(pkt) {
                                    Ok(i) => Ok(i),
                                    Err(e) => Err(e.into())
                                }
                            }).await;
                        } else {
                            packet_handler.write(chunk);
                            while let Some(pkt) = packet_handler.flush_packet_for_vbr() {
                                sent_bytes += &pkt.len();
                                let _ = tx.send(pkt);
                            }
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        packet_handler.helper.log(&format!("lagged {}", n));
                    },
                    Err(e) => {
                        packet_handler.helper.log(&format!("received failed:{:?}", e));
                        continue;
                    }
                }
            },
            _ = timer_stats.tick() => {
                packet_handler.update_stats();
                packet_handler.stats.update_bitrate(sent_bytes as u64 * 8);
                sent_bytes = 0;
            }
        }
    }

    if let Err(e) = misc::wait_and_abort(accept_task).await {
        packet_handler.helper.log(&format!("Force listening shutdown: {:?}", e));
    }

    if let Err(e) = fs::remove_file(&socket_path).await {
        return Err(anyhow!("Error removing socket file: {:?}", e));
    }

    Ok(())
}
