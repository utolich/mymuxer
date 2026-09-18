use crate::api_client::ApiClient;
use crate::mux::OutBuffer;
use crate::{config, packet};
use anyhow::Result;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::process::Output;
use std::sync::RwLock;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use strum::Display;
use tokio::process::Command;

#[cfg(unix)]
use tikv_jemalloc_ctl::{epoch, stats};

#[derive(Display, Serialize)]
pub enum ApiActions {
    #[strum(serialize = "status")]
    Status,
    #[strum(serialize = "event")]
    Event,
    #[strum(serialize = "log")]
    Log,
    #[strum(serialize = "cmd")]
    Cmd,
}

#[derive(Serialize)]
pub struct BufferStats {
    pub(crate) size: AtomicUsize,
    pub(crate) duration: AtomicU64,
}

#[derive(Serialize)]
pub struct InputStats {
    pub(crate) start_timestamp: AtomicI64,
    pub(crate) bitrate: AtomicU64,
    pub(crate) max_bitrate: AtomicU64,
}

impl InputStats {
    pub fn new() -> Self {
        Self {
            start_timestamp: AtomicI64::new(Utc::now().timestamp()),
            bitrate: AtomicU64::new(0),
            max_bitrate: AtomicU64::new(0),
        }
    }

    pub fn update_bitrate(&self, bitrate: u64) {
        self.bitrate.store(bitrate, Ordering::Relaxed);
        let max_bitrate = self.max_bitrate.load(Ordering::Relaxed);
        self.max_bitrate
            .store(max(bitrate, max_bitrate), Ordering::Relaxed);
    }
}

#[derive(Serialize)]
pub struct OutputStats {
    pub(crate) start_timestamp: AtomicI64,
    pub(crate) last_ready_timestamp: AtomicI64,
    pub(crate) in_buffer_len: AtomicU64,

    pub(crate) bitrate: AtomicU64,
    pub(crate) bitrate_psi: AtomicU64,
    pub(crate) bitrate_payload: AtomicU64,
    pub(crate) bitrate_null: AtomicU64,
    pub(crate) bitrate_adjust_payload: AtomicU64,
    pub(crate) bitrate_adjust_null: AtomicU64,
    pub(crate) fps: AtomicU64,

    pub(crate) v_buffer_stats: BufferStats,
    pub(crate) a_buffer_stats: BufferStats,

    pub(crate) dts_drift: RwLock<HashMap<u16, i64>>,
    pub(crate) pcr_drift: AtomicI64,
    pub(crate) buffer_duration_ms: AtomicU64,
    pub(crate) jitter_ms: AtomicU64,
    pub(crate) adjust_buf: AtomicU64,
    pub(crate) cc: RwLock<HashMap<u16, usize>>,
    pub(crate) dirty: AtomicUsize,
}

impl OutputStats {
    pub fn new() -> Self {
        Self {
            start_timestamp: AtomicI64::new(Utc::now().timestamp()),
            last_ready_timestamp: AtomicI64::new(Utc::now().timestamp()),
            in_buffer_len: AtomicU64::new(0),

            bitrate: AtomicU64::new(0),
            bitrate_psi: AtomicU64::new(0),
            bitrate_payload: AtomicU64::new(0),
            bitrate_null: AtomicU64::new(0),
            bitrate_adjust_payload: AtomicU64::new(0),
            bitrate_adjust_null: AtomicU64::new(0),
            fps: AtomicU64::new(0),

            v_buffer_stats: BufferStats {
                size: AtomicUsize::new(0),
                duration: AtomicU64::new(0),
            },
            a_buffer_stats: BufferStats {
                size: AtomicUsize::new(0),
                duration: AtomicU64::new(0),
            },
            dts_drift: RwLock::new(HashMap::new()),
            pcr_drift: AtomicI64::new(0),
            buffer_duration_ms: AtomicU64::new(0),
            jitter_ms: AtomicU64::new(0),
            adjust_buf: AtomicU64::new(0),
            cc: RwLock::new(HashMap::new()),
            dirty: AtomicUsize::new(0),
        }
    }
    pub fn update_in_buffer(&self, in_buff: &BytesMut) {
        self.in_buffer_len
            .store(in_buff.len() as u64, Ordering::Relaxed);
    }
    pub fn update_bitrate(&self, bitrate: u64) {
        self.bitrate.store(bitrate, Ordering::Relaxed);
    }

    pub fn update_bitrate_psi(&self, count_packets: &mut usize) {
        let bitrate = *count_packets * 8 * packet::TS_PACKET_SIZE;
        self.bitrate_psi.store(bitrate as u64, Ordering::Relaxed);
        *count_packets = 0;
    }

    pub fn update_bitrate_payload(&self, count_packets: &mut usize) {
        let bitrate = *count_packets * 8 * packet::TS_PACKET_SIZE;
        self.bitrate_payload
            .store(bitrate as u64, Ordering::Relaxed);
        *count_packets = 0;
    }

    pub fn update_bitrate_null(&self, count_packets: &mut usize) {
        let bitrate = *count_packets * 8 * packet::TS_PACKET_SIZE;
        self.bitrate_null.store(bitrate as u64, Ordering::Relaxed);
        *count_packets = 0;
    }

    pub fn update_bitrate_adjust_null(&self, count_packets: &mut usize) {
        let bitrate = *count_packets * 8 * packet::TS_PACKET_SIZE;
        self.bitrate_adjust_null.store(bitrate as u64, Ordering::Relaxed);
        *count_packets = 0;
    }

    pub fn update_bitrate_adjust_payload(&self, count_packets: &mut usize) {
        let bitrate = *count_packets * 8 * packet::TS_PACKET_SIZE;
        self.bitrate_adjust_payload.store(bitrate as u64, Ordering::Relaxed);
        *count_packets = 0;
    }

    pub fn update_fps(&self, fps: &f64) {

        self.fps.store((fps * 100.0) as u64, Ordering::Relaxed);
    }

    pub fn update_dts_drift(&self, pid: u16, dts_drift: i64) {
        if let Ok(mut guard) = self.dts_drift.write() {
            guard.insert(pid, dts_drift);
        }
    }

    pub fn update_pcr_drift(&self, pcr_drift: i64) {
        self.pcr_drift.store(pcr_drift, Ordering::Relaxed);
    }
    pub fn update_buffer_duration(&self, duration: u64) {
        self.buffer_duration_ms.store(duration, Ordering::Relaxed);
    }
    pub fn update_jitter(&self, jitter: u64) {
        self.jitter_ms.store(jitter, Ordering::Relaxed);
    }
    pub fn update_adjust_buf(&self, adjust_buf: u64) {
        self.adjust_buf.store(adjust_buf, Ordering::Relaxed);
    }

    pub fn update_cc(&self, cc: &HashMap<u16, usize>) {
        if let Ok(mut guard_cc) = self.cc.write() {
            for (pid, new_count) in cc {
                guard_cc.insert(*pid, *new_count);
            }
        }
    }

    pub fn update_dirty(&self, dirty: bool) {
        self.dirty.store(dirty as usize, Ordering::Relaxed);
    }

    pub fn update_ready_time(&self, last_ready_time: DateTime<Utc>) {
        self.last_ready_timestamp
            .store(last_ready_time.timestamp(), Ordering::Relaxed);
    }

    pub fn update_v_buffer(&self, buffer: &OutBuffer) {
        self.v_buffer_stats
            .size
            .store(buffer.size(), Ordering::Relaxed);
        self.v_buffer_stats
            .duration
            .store(buffer.duration_ms(), Ordering::Relaxed);
    }

    pub fn update_a_buffer(&self, buffer: &OutBuffer) {
        self.a_buffer_stats
            .size
            .store(buffer.size(), Ordering::Relaxed);
        self.a_buffer_stats
            .duration
            .store(buffer.duration_ms(), Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct ApiConfig {
    pub(crate) api_client: Option<ApiClient>,
    pub(crate) admin_url: String,
    pub(crate) hash: String,
    pub(crate) last_update: String,
}

fn api_data(api_config: &ApiConfig, action: ApiActions, data: serde_json::Value) -> Result<String> {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let data = json!(
    {
        "hash": api_config.hash,
        "last_update": api_config.last_update,
        "time": now.to_string(),
        "action": action.to_string(),
        "data": data
    });
    Ok(serde_json::to_string(&data)?)
}

pub fn api_data_status(api_config: &ApiConfig, data: serde_json::Value) -> Result<String> {
    api_data(api_config, ApiActions::Status, data)
}

pub fn api_data_event(api_config: &ApiConfig, data: serde_json::Value) -> Result<String> {
    api_data(api_config, ApiActions::Event, data)
}

pub fn api_data_log(api_config: &ApiConfig, data: serde_json::Value) -> Result<String> {
    api_data(api_config, ApiActions::Log, data)
}

pub fn api_data_cmd(api_config: &ApiConfig, data: serde_json::Value) -> Result<String> {
    api_data(api_config, ApiActions::Cmd, data)
}

pub async fn shell_top() -> Option<Output> {
    let pidfile = config::pid_filename().await;
    match fs::exists(&pidfile) {
        Ok(true) => Command::new("sh")
            .arg("-c")
            .arg(format!("top -n -d 2 -p `cat {}`| awk 'BEGIN{{found=0}} /last pid/{{found++}}  {{if (found==2) print }}'| head -n 10", pidfile.to_str()?))
            .output()
            .await
            .ok(),
        _ => Command::new("sh")
            .arg("-c")
            .arg("top -n -d 2 | awk 'BEGIN{found=0} /last pid/{found++}  {if (found==2) print }'| head -n 7")
            .output()
            .await
            .ok()

    }
}

pub fn memory_stats() -> (libc::size_t, libc::size_t, libc::size_t) {
    #[cfg(not(unix))]
    {
        return (0, 0, 0);
    }

    #[cfg(unix)]
    {
        epoch::advance().unwrap();
        let active = stats::active::read().unwrap();
        let resident = stats::resident::read().unwrap();
        let allocated = stats::allocated::read().unwrap();

        (active, resident, allocated)
    }
}