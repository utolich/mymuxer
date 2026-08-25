use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::collections::{BTreeMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use anyhow::{anyhow, Result, Context};
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;
use crate::config::EpgConfig;
use crate::packet::TS_PACKET_SIZE;
use crate::{misc, packet, psi};
use crate::status::ApiConfig;

const EIT_PERIOD_MS: u64 = 1_000;
const TDT_PERIOD_MS: u64 = 10_000;
const EIT_TS_PACKET_COUNT: usize = 7;
const EIT_TS_PACKET_SIZE: usize = EIT_TS_PACKET_COUNT * TS_PACKET_SIZE;
const MIN_TICK_NS: u64 = 1_000_000;

pub struct EpgTask {
    pub(crate) handle: JoinHandle<()>,
    pub(crate) cancel: CancellationToken,
}

#[derive(Debug, Deserialize, Hash)]
struct EpgEvents {
    version: u8,
    generated_at_utc: DateTime<Utc>,
    network: EpgNetwork,
    events: Vec<EpgEvent>,
}

#[derive(Debug, Clone, Deserialize, Hash)]
struct EpgNetwork {
    country_code: String,
    timezone: String,
}

#[derive(Debug, Clone, Deserialize, Hash)]
struct EpgEvent {
    original_network_id: u16,
    transport_stream_id: u16,
    service_id: u16,
    event_id: u16,
    start_time_utc: DateTime<Utc>,
    duration_seconds: u32,
    title: String,
    #[serde(default)]
    subtitle: Option<String>,
    #[serde(default)]
    description: Option<String>,
    language: String,
}

#[derive(Debug, Clone)]
struct EpgState {
    current_version: u8,
    generated_at_utc: DateTime<Utc>,
    last_hash: u64
}

impl EpgState {
    fn new(
        current_version: u8,
        generated_at_utc: DateTime<Utc>,
        last_hash: u64,
    ) -> Self {
        Self {
            current_version,
            generated_at_utc,
            last_hash,
        }
    }
}

pub(crate) async fn run(cancel: CancellationToken, start_config: EpgConfig, api_config: ApiConfig) -> Result<()> {
    let url = start_config.url.as_str();
    let dest =  parse_udp_url(url)?;
    let id = start_config.id;

    info!("EPG #{} started", id);

    let now = Instant::now();
    let last_download = Arc::new(RwLock::new(now));
    let last_process = Arc::new(RwLock::new(now));
    let last_send = Arc::new(RwLock::new(now));

    let last_download_clone = Arc::clone(&last_download);
    let last_process_clone = Arc::clone(&last_process);
    let last_send_clone = Arc::clone(&last_send);
    let monitor_cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            if {
                let guard = last_download_clone.read().await;
                guard.elapsed()
            } > tokio::time::Duration::from_secs(20) {
                error!("EPG download is too rarely");
                break;
            }
            if {
                let guard = last_send_clone.read().await;
                guard.elapsed()
            } > tokio::time::Duration::from_secs(20) {
                if {
                    let guard = last_process_clone.read().await;
                    guard.elapsed()
                } < tokio::time::Duration::from_secs(20) {
                    error!("EPG sent is too rarely");
                    break;
                }
            }

            if {
                let guard = last_process_clone.read().await;
                guard.elapsed()
            } > tokio::time::Duration::from_mins(60) {
                error!("EPG processed is too rarely");
                break;
            }

            if misc::wait_interval(&monitor_cancel, 5f32).await.is_err() {
                break;
            };
        }

        monitor_cancel.cancel();
    });

    let (epg_state, epg_network, epg_events) = loop {
        if let Ok((epg_state, epg_network, epg_events)) = load_epg_events(&api_config, &id).await {
            if !epg_events.is_empty() {
                break (epg_state, epg_network, epg_events);
            }
        }

        if misc::wait_interval(&cancel, 10f32).await.is_err() {
            return Ok(());
        };
    };

    let mut current_epg_state = EpgState::new(
        epg_state.current_version,
        epg_state.generated_at_utc,
        epg_state.last_hash,
    );

    let epg_services = Arc::new(RwLock::new(Arc::new((epg_state, epg_network, group_events_by_service(epg_events)))));
    let bind_addr = if dest.is_ipv4() {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
    } else {
        return Err(anyhow!("IPv6 destinations are not supported by this test generator"));
    };
    let socket = UdpSocket::bind(bind_addr).await?;

    let load_cancel = cancel.clone();
    let load_epg_services = Arc::clone(&epg_services);

    let last_download_clone = Arc::clone(&last_download);

    let _load_handle = tokio::spawn(async move {
        let mut timer_load_events = tokio::time::interval(std::time::Duration::from_secs(5));
        let last_download_clone = Arc::clone(&last_download_clone);
        loop {
            tokio::select! {
                _ = load_cancel.cancelled() => {
                    break;
                },
                _ = timer_load_events.tick() => {
                    match load_epg_events(&api_config, &id).await {
                        Ok((new_epg_state, new_epg_network, new_epg_events)) => {
                            {
                                let mut guard = last_download_clone.write().await;
                                *guard = Instant::now();
                            }
                            let group_epg_events = group_events_by_service(new_epg_events);
                            let mut guard = load_epg_services.write().await;
                            *guard = Arc::new((new_epg_state, new_epg_network, group_epg_events));
                        },
                        Err(e) => {
                            error!("EPG load failed: {:?}", e);
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        }
        error!("EPG load task exited");
    });

    let mut last_tdt = Instant::now();

    let mut eit_cc = 0;
    let mut tdt_tot_cc = 0;

    let mut packets = VecDeque::new();

    let last_process_clone = Arc::clone(&last_process);
    loop {
        let now = Utc::now();
        let mut sections = Vec::new();
        let epg_services_clone = {
            let guard = epg_services.read().await;
            Arc::clone(&guard)
        };

        let (new_epg_state, new_epg_network, new_epg_events) = epg_services_clone.as_ref();

        if new_epg_state.last_hash != current_epg_state.last_hash {
            current_epg_state.current_version = (current_epg_state.current_version + 1) & 0x1F;
            current_epg_state.last_hash = new_epg_state.last_hash;
        }
        current_epg_state.generated_at_utc = new_epg_state.generated_at_utc;

        let instant_now = Instant::now();
        if instant_now.duration_since(last_tdt).as_millis() >= TDT_PERIOD_MS as u128 {
            for section in [
                psi::tdt::section(now),
                psi::tot::section(now, &new_epg_network.country_code, &new_epg_network.timezone)
            ] {
                for packet in psi::pack_multi(&section.bytes, section.pid, &mut tdt_tot_cc) {
                    packets.push_back(packet);
                }
            }
            last_tdt = instant_now;
        }

        for ((original_network_id, transport_stream_id, service_id), events) in new_epg_events {
            let Some((present, following)) = present_following_events(events, now) else {
                continue;
            };

            {
                let mut guard = last_process_clone.write().await;
                *guard = Instant::now();
            }

            sections.push(psi::eit::present_following_section(
                *service_id,
                current_epg_state.current_version,
                *transport_stream_id,
                *original_network_id,
                0,
                present.event_id,
                present.start_time_utc,
                Duration::seconds(present.duration_seconds as i64),
                &present.title,
                &event_description(present),
                &present.language
            ));
            if let Some(following) = following {
                sections.push(psi::eit::present_following_section(
                    *service_id,
                    current_epg_state.current_version,
                    *transport_stream_id,
                    *original_network_id,
                    1,
                    following.event_id,
                    following.start_time_utc,
                    Duration::seconds(following.duration_seconds as i64),
                    &following.title,
                    &event_description(following),
                    &present.language
                ));
            } else {
                sections.push(psi::eit::empty_present_following_section(
                    *service_id,
                    current_epg_state.current_version,
                    *transport_stream_id,
                    *original_network_id,
                    1,
                ));
            }
        }

        for section in sections {
            for packet in psi::pack_multi(&section.bytes, section.pid, &mut eit_cc) {
                packets.push_back(packet);
            }
        }

        if packets.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
            continue;
        }

        let time_ns: u64 = (EIT_PERIOD_MS * 1_000_000 * EIT_TS_PACKET_COUNT as u64 / packets.len() as u64).max(MIN_TICK_NS);
        let mut timer = tokio::time::interval(tokio::time::Duration::from_nanos(time_ns));

        let mut sent_bytes = 0;
        let last_send_clone = Arc::clone(&last_send);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return Ok(());
                },
                _ = timer.tick() => {
                    if packets.len() < EIT_TS_PACKET_COUNT {
                        break;
                    }
                    let mut out = Vec::with_capacity(EIT_TS_PACKET_SIZE);
                    for _ in 0 .. EIT_TS_PACKET_COUNT {
                        let pkt = packets.pop_front().unwrap_or_else(|| packet::TS_NULL_PACKET);
                        out.extend_from_slice(&pkt);
                    };
                    match tokio::time::timeout(tokio::time::Duration::from_secs(3), socket.send_to(&out, dest)).await {
                        Ok(Ok(bytes)) => {
                            {
                                let mut guard = last_send_clone.write().await;
                                *guard = Instant::now();
                            }
                            sent_bytes += bytes;
                        },
                        Ok(Err(e)) => {
                            error!("EPG send failed: {:?}", e);
                            if misc::wait_interval(&cancel, 5f32).await.is_err() {
                                return Ok(());
                            };
                            break;
                        },
                        Err(_) => {
                            error!("EPG send timeout");
                            break;
                        }
                    };
                }
            }
        }
    }
}

fn parse_udp_url(value: &str) -> Result<SocketAddr> {
    let url = Url::parse(value).context("destination must look like udp://239.200.99.1:1234")?;
    if url.scheme() != "udp" {
        return Err(anyhow!("destination scheme must be udp"));
    }
    let host = url.host_str().ok_or_else(|| anyhow!("missing UDP host"))?;
    let port = url.port().ok_or_else(|| anyhow!("missing UDP port"))?;
    format!("{}:{}", host, port)
        .parse()
        .context("invalid UDP socket address")
}

async fn load_epg_events(api_config: &ApiConfig, id: &u32) -> Result<(EpgState, EpgNetwork, Vec<EpgEvent>)> {
    if api_config.api_client.is_none() {
        return Err(anyhow!("api_client is not set"));
    }

    let api_client = api_config.api_client.as_ref().unwrap();

    let mut admin_url = Url::parse(&api_config.admin_url)?;
    admin_url.set_path(&format!("/api/epg/id/{}", id));


    let epg_json = api_client.get(admin_url).await?;

    let epg: EpgEvents = serde_json::from_str(&epg_json)
        .with_context(|| format!("failed to parse {:?}", epg_json))?;

    let mut events = epg.events;
    events.sort_by_key(|event| event.start_time_utc);

    let mut hasher = DefaultHasher::new();
    events.hash(&mut hasher);
    let hash = hasher.finish();

    let network = epg.network;
    if network.country_code.len() != 3 {
        return Err(anyhow!("invalid country code"));
    }

    let epg_state = EpgState::new(
        epg.version,
        epg.generated_at_utc,
        hash,
    );

    Ok((epg_state, network, events))
}

fn group_events_by_service(
    events: Vec<EpgEvent>,
) -> BTreeMap<(u16, u16, u16), Vec<EpgEvent>> {
    let mut grouped = BTreeMap::new();
    for event in events {
        grouped
            .entry((
                event.original_network_id,
                event.transport_stream_id,
                event.service_id,
            ))
            .or_insert_with(Vec::new)
            .push(event);
    }

    for events in grouped.values_mut() {
        events.sort_by_key(|event| event.start_time_utc);
    }

    grouped
}

fn present_following_events(
    events: &[EpgEvent],
    now: DateTime<Utc>,
) -> Option<(&EpgEvent, Option<&EpgEvent>)> {
    let present_index = events.iter().position(|event| {
        let end = event.start_time_utc + Duration::seconds(event.duration_seconds as i64);
        event.start_time_utc <= now && now < end
    });

    if let Some(index) = present_index {
        let following = events.get(index + 1);
        return Some((&events[index], following));
    }

    let mut upcoming = events.iter().filter(|event| event.start_time_utc > now);
    match (upcoming.next(), upcoming.next()) {
        (Some(present), following) => Some((present, following)),
        _ if !events.is_empty() => Some((&events[0], events.get(1))),
        _ => None,
    }
}

fn event_description(event: &EpgEvent) -> String {
    if let Some(desc) = &event.description {
        if !desc.is_empty() {
            return desc.to_string();
        }
    }
    if let Some(sub) = &event.subtitle {
        if !sub.is_empty() {
            return sub.to_string();
        }
    }

    "".to_string()
}
