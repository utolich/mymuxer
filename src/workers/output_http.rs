use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use http_body_util::StreamBody;
use hyper::body::{Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;

use crate::config;
use crate::mux::PacketHandler;
use crate::workers::{Helper, OutputStats, Worker};

const BROADCAST_CAPACITY: usize = 256;

pub async fn output(
    rtx: broadcast::Sender<Bytes>,
    output: config::Output,
    cancel: CancellationToken,
    mut helper: Helper,
    stats: Arc<OutputStats>,
) -> Result<()> {
    helper.set_output_id(output.id);
    helper.set_namespace(&format!("Output http #{}", output.id));
    helper.log_play(&format!("Output http {} running", output.output_url));

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
    let url = &packet_handler.output.output_url;

    let listener = TcpListener::bind(url).await?;

    let connections: Arc<RwLock<HashSet<SocketAddr>>> = Arc::new(RwLock::new(HashSet::new()));

    let (tx, _rx) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);
    //drop(_rx);

    let mut rrx = rtx.subscribe();

    let cancel_accept = cancel.clone();
    let tx_accept = tx.clone();
    let mut helper_accept = packet_handler.helper.clone();
    let connections_accept = Arc::clone(&connections);
    let accept_task = tokio::spawn(async move {
        let mut err_count = 0;
        loop {
            tokio::select! {
                biased;
                _ = cancel_accept.cancelled() => {
                    helper_accept.log_pause("Output http listen stoping");
                    break;
                },
                res = listener.accept() => {
                    match res {
                        Ok((stream, addr)) => {
                            err_count = 0;
                            helper_accept.log(&format!("New connection from {}", addr));
                            connections_accept.write().await.insert(addr);

                            let cancel_clone = cancel_accept.clone();
                            let tx_clone = tx_accept.clone();
                            let mut helper_clone = helper_accept.clone();
                            let connections_clone = Arc::clone(&connections_accept);
                            tokio::spawn(async move {
                                let io = TokioIo::new(stream);
                                let svc = service_fn(move |req| handler(req, tx_clone.clone()));
                                let http = http1::Builder::new();
                                tokio::select! {
                                    biased;
                                    _ = cancel_clone.cancelled() => {
                                        helper_clone.log(&format!("Closing connection {addr}"));
                                    },
                                    res = http.serve_connection(io, svc) => {
                                        if let Err(err) = res {
                                            helper_clone.log(&format!("Connection error {addr}: {err:?}"));
                                        }
                                    }
                                }
                                connections_clone.write().await.remove(&addr);
                            });
                        },
                        Err(e) => {
                            if err_count == 0 {
                                helper_accept.log_warn(&format!("Accept new connection error: {:?}", e));
                            } else {
                                break;
                            }
                            err_count += 1;
                        }
                    }
                }
            }
        }
        helper_accept.cmd_mark_dirty();
    });


    let mut timer_stats = tokio::time::interval(std::time::Duration::from_secs(1));
    timer_stats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut sent_bytes: usize = 0;

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                packet_handler.helper.log_pause("Output http stoping");
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
                            while let Some(pkt) = packet_handler.flush_frame() {
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

    if let Err(e) = Worker::wait_and_abort(accept_task).await {
        packet_handler.helper.log(&format!("Force listening shutdown: {:?}", e));
    }
    
    Ok(())
}

async fn handler(
    _req: Request<Incoming>,
    tx: broadcast::Sender<Bytes>,
) -> Result<Response<StreamBody<impl Stream<Item = Result<Frame<Bytes>>>>>> {
    let rx = tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |item| async move {
        match item {
            Ok(bytes) => Some(Ok(Frame::data(bytes))),
            Err(_) => None,
        }
    });

    let body = StreamBody::new(stream);
    let response = Response::new(body);

    Ok(response)
}
