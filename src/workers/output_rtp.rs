use crate::mux::PacketHandler;
use crate::proto::rtp::RtpState;
use crate::workers::{Helper, OutputStats};
use crate::config;
use anyhow::Result;
use bytes::Bytes;
use socket2::SockRef;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const CONNECTIONS_REPEAT: usize = 3;

const CONNECTIONS_TIMEOUT_SEC: u64 = 3;

const RTP_SEND_BUFFER_BYTES: usize = 512 * 1024;

pub async fn output(
    tx: broadcast::Sender<Bytes>,
    output: config::Output,
    cancel: CancellationToken,
    mut helper: Helper,
    stats: Arc<OutputStats>,
) -> Result<()> {
    helper.set_output_id(output.id);
    helper.set_namespace(&format!("Output rtp #{}", output.id));
    helper.log_play(&format!("Output rtp {} running", output.output_url));

    let packet_handler_helper = helper.clone();

    if let Some(packet_handler) = PacketHandler::new(output, packet_handler_helper, stats) {
        if packet_handler.output.proxy {
            match proxy(tx, packet_handler, cancel).await {
                Ok(_) => {}
                Err(e) => {
                    helper.log(&format!("{:?}", e));
                }
            }
        } else {
            match cbr(tx, packet_handler, cancel).await {
                Ok(_) => {}
                Err(e) => {
                    helper.log(&format!("{:?}", e));
                }
            }
        }
    } else {
        println!("No program found");
    }

    Ok(())
}

async fn proxy(
    tx: broadcast::Sender<Bytes>,
    mut packet_handler: PacketHandler,
    cancel: CancellationToken,
) -> Result<()> {
    let mut rx = tx.subscribe();
    let url = packet_handler.output.output_url.clone();
    let stop = cancel.clone();

    let mut timer_stats = tokio::time::interval(std::time::Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let sock = Arc::new(connect(&url, &mut packet_handler.helper.clone()).await?);

    let mut sent_bytes: usize = 0;

    let rtp_state = Arc::new(RtpState::new());

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(chunk) => {
                        sent_bytes += packet_handler.proxy_packet(chunk, |pkt| {
                            let s = Arc::clone(&sock);
                            let stop = stop.clone();
                            let rtp_state = Arc::clone(&rtp_state);
                            async move {
                                let rtp_pkt = rtp_state.pack(&pkt)?;
                                match s.send(&rtp_pkt).await {
                                    Ok(i) => Ok(i),
                                    Err(e) => {
                                        stop.cancel();
                                        Err(e.into())
                                    }
                                }
                            }
                        }).await;
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
            },
            _ = cancel.cancelled() => {
                packet_handler.helper.log_pause("Output rtp (proxy): stopping");
                break;
            }
        }
    }
    Ok(())
}

async fn cbr(
    tx: broadcast::Sender<Bytes>,
    mut packet_handler: PacketHandler,
    cancel: CancellationToken,
) -> Result<()> {
    let mut rx = tx.subscribe();
    let url = packet_handler.output.output_url.clone();

    let pkt_size: u16 = packet_handler.get_pkt_size();

    let mut bitrate = packet_handler.get_bitrate().unwrap_or(0); // bits per second
    if bitrate == 0 {
        packet_handler.helper.log_info("Calculating bitrate...");
        while bitrate == 0 {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    packet_handler.helper.log_pause("Output udp (cbr): stopping");
                    break;
                },
                res = rx.recv() => {
                    match res {
                        Ok(chunk) => {
                            packet_handler.write(chunk);
                            bitrate = packet_handler.get_bitrate().unwrap_or(0); // bits per second
                        },
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            packet_handler.helper.log_warn(&format!("Output udp (cbr): lagged {}", n));
                        },
                        Err(e) => {
                            packet_handler.helper.log_warn(&format!("Output udp (cbr): received failed: {:?}", e));
                            continue;
                        }
                    }
                },
            }
        }
        packet_handler.helper.log_info(&format!("Set bitrate {} bit/s", bitrate));
    }

    if bitrate == 0 {
        return Err(anyhow::anyhow!("cbr bitrate is zero"));
    }
    let time_ns: u64 = 1_000_000_000 * pkt_size as u64 * 8 / bitrate; // nanoseconds
    let mut timer = tokio::time::interval(Duration::from_nanos(time_ns));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let mut timer_stats = tokio::time::interval(Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let sock = connect(&url, &mut packet_handler.helper.clone()).await?;

    let mut sent_bytes: usize = 0;
    let mut _ticks: usize = 0;

    packet_handler.set_check_buffer_size();
    packet_handler.set_buffer_process_async();

    let rtp_state = RtpState::new();

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                packet_handler.helper.log_pause("Output rtp (cbr): stopping");
                break;
            },
            _ = timer.tick() => {
                _ticks += 1;
                if let Some(pkt) = packet_handler.flush_packet_for_cbr() {
                    let rtp_pkt = rtp_state.pack(&pkt)?;
                    match sock.send(&rtp_pkt).await {
                        Ok(i) => {
                            sent_bytes += i;
                        },
                        Err(e) => {
                            packet_handler.helper.log_warn(&format!("Output rtp (cbr): send failed:{:?}", e));
                            break;
                        }
                    }
                }
            },
            _ = timer_stats.tick() => {
                packet_handler.update_stats();
                packet_handler.stats.update_bitrate(sent_bytes as u64 * 8);
                sent_bytes = 0;
                _ticks = 0;
            },
            _ = packet_handler.buffer_process() => {},
            res = rx.recv() => {
                match res {
                    Ok(chunk) => {
                        packet_handler.write(chunk);
                    },
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        packet_handler.helper.log_warn(&format!("Output rtp (cbr): lagged {}", n));
                    },
                    Err(e) => {
                        packet_handler.helper.log_warn(&format!("Output rtp (cbr): received failed: {:?}", e));
                        continue;
                    }
                }
            },
        }
    }
    Ok(())
}

async fn connect(url: &str, helper: &mut Helper) -> Result<UdpSocket> {
    for _i in 1..=CONNECTIONS_REPEAT {
        match UdpSocket::bind("0.0.0.0:0").await {
            Ok(sock) => {
                let sock_ref = SockRef::from(&sock);

                if let Err(e) = sock_ref.set_send_buffer_size(RTP_SEND_BUFFER_BYTES) {
                    return Err(anyhow::anyhow!(format!("Set send buffer failure {}", e)));
                }

                if let Ok(size) = sock_ref.send_buffer_size() {
                    helper.log(&format!("Send buffer size: {} bytes", size));
                }

                sock.set_multicast_loop_v4(true)?;
                sock.set_multicast_ttl_v4(32)?;

                if let Err(e) = sock.connect(url).await {
                    helper.log(&format!("Connection error: {}", e));
                    tokio::time::sleep(Duration::from_secs(CONNECTIONS_TIMEOUT_SEC)).await;
                    return Err(e.into());
                }

                return Ok(sock);
            }
            Err(_e) => {
                tokio::time::sleep(Duration::from_secs(CONNECTIONS_TIMEOUT_SEC)).await;
            }
        }
    }
    Err(anyhow::anyhow!("Connection error"))
}
