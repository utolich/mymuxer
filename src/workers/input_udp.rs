use crate::{config, mux};
use crate::workers::{InputStats, Helper};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use reqwest::Url;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::UdpSocket as StdUdpSocket;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::sync::CancellationToken;

const UDP_RECV_BUFFER_BYTES: usize = 5 * 1024 * 1024;
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
    helper.set_namespace(&format!("Input udp #{}", worker_id));
    helper.log_play(&format!("Input udp {} running", url));

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
            res = connect(url, &mut helper) => res
        };
        match response {
            Ok(res) => {
                let mut last_chunk_at = Instant::now();
                let input_timeout = Duration::from_secs(5);
                let mut buffer = [0u8; READ_CHUNK_SIZE];
                let mut in_buff = BytesMut::with_capacity(IN_BUFFER_SIZE);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            helper.log_pause("Input udp stopped");

                            break 'send;
                        }
                        res = timeout(input_timeout, res.recv_from(&mut buffer)) => {
                            match res {
                                Ok(Ok((n, _address))) => {
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
                            }
                        },
                        _ = timer_stats.tick() => {
                            let bitrate: u64 = (sent_bytes * 8) as u64;
                            stats.update_bitrate(bitrate);
                            sent_bytes = 0;
                        }
                    }
                }
                drop(res);
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

async fn connect(url: &str, helper: &mut Helper) -> Result<UdpSocket> {
    let u = match Url::parse(url) {
        Ok(u) => u,
        Err(_e) => return Err(anyhow::anyhow!(format!("Invalid url {}", url))),
    };

    match (u.host_str(), u.port()) {
        (Some(host), Some(port)) => {
            let multi_addr = Ipv4Addr::from_str(host)?;
            let raw_socket = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)) {
                Ok(raw_socket) => raw_socket,
                Err(e) => return Err(anyhow::anyhow!(format!("Set receive buffer failure {}", e))),
            };

            if let Err(e) = raw_socket.set_recv_buffer_size(UDP_RECV_BUFFER_BYTES) {
                return Err(anyhow::anyhow!(format!("Set receive buffer failure {}", e)));
            };

            if let Ok(size) = raw_socket.recv_buffer_size() {
                helper.log(&format!("Receive buffer size: {} bytes", size));
            };

            if let Err(e) = raw_socket.set_reuse_address(true) {
                helper.log(&format!("Set reuse address failure {}", e));
            };

            #[cfg(all(unix, not(target_os = "solaris")))]
            if let Err(e) = raw_socket.set_reuse_port(true) {
                helper.log(&format!("Set reuse port failure {}", e));
            };

            let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
            if let Err(e) = raw_socket.bind(&bind_addr.into()) {
                return Err(anyhow::anyhow!(format!(
                    "Bind address {} failure {}",
                    bind_addr, e
                )));
            };

            if let Err(e) = raw_socket.join_multicast_v4(&multi_addr, &Ipv4Addr::UNSPECIFIED) {
                return Err(anyhow::anyhow!(format!("Join multicast failure {}", e)));
            };

            let std_socket: StdUdpSocket = raw_socket.into();

            if let Err(e) = std_socket.set_nonblocking(true) {
                return Err(anyhow::anyhow!(format!("Set non blocking failure {}", e)));
            };

            match UdpSocket::from_std(std_socket) {
                Ok(sock) => Ok(sock),
                Err(e) => Err(anyhow::anyhow!(format!(
                    "UdpSocket conversion failure {}",
                    e
                ))),
            }
        }
        _ => Err(anyhow::anyhow!("Invalid url {}", url)),
    }
}
