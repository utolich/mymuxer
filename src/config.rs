use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json;
use std::env;
use std::path::PathBuf;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

const PID_FILE: &str = "pid/mymuxer.pid";
const CONFIG_FILE: &str = "config/config.json";
pub(crate) const DATABASE_FILE: &str = "data/mymuxer.redb";
pub(crate) const LOG_DIR: &str = "logs";
pub(crate) const STREAMS_LOG_DIR: &str = "logs/streams";
pub(crate) const ALLOWED_TASK_TYPES: [&str; 6] = ["udp", "rtp", "http", "ffmpeg", "hls", "unix"];
pub(crate) const CA_FLAG_UNSCRAMBLED: usize = 1;

#[derive(Debug, Deserialize, Clone)]
pub struct HttpRouterConfig {
    pub id: u32,
    pub url: String,
    pub routes: Vec<String>,
    pub allow: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EpgConfig {
    pub id: u32,
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub mgnt: Mgnt,
    pub epg_configs: Vec<EpgConfig>,
    pub streams: Vec<Stream>,
    pub http_routers: Vec<HttpRouterConfig>
}

#[derive(Debug, Deserialize, Clone)]
pub struct Mgnt {
    pub hash: String,
    pub last_update: f64,
    pub admin_url: String,
    pub allow: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Stream {
    pub id: u32,
    pub remote_url: String,
    pub biss_key: String,
    pub name: String,
    pub task_type: String,
    pub outputs: Vec<Output>,
    pub err_count: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Output {
    pub id: u32,
    pub name: String,
    pub output_url: String,
    pub task_type: String,
    pub biss_key: String,
    pub ca_flag: usize,
    pub pkt_size: u16,
    pub cbr: u32,
    pub proxy: bool,
    pub allow: Vec<String>,
    pub programs: Vec<Program>,
    pub err_count: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Program {
    pub program_id: u16,
    pub audio_pids: Vec<u16>,
    pub video_pids: Vec<u16>,
    pub pmt_pid: u16,
    pub pcr_pid: u16,
    pub service_name: String,
    pub service_provider_name: String,
    pub service_type: u32,
    pub buffer_duration: u32,
    pub jitter: u32,
    pub adjust_buf: u32,
}

impl Config {
    pub fn get_stream(&self, id: u32) -> Option<&Stream> {
        self.streams.iter().find(|s| s.id == id)
    }
}

pub(crate) fn project_dir() -> Option<PathBuf> {
    if let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR") {
        Some(PathBuf::from(manifest_dir))
    } else if let Ok(mut exe_path) = env::current_exe() {
        exe_path.pop();
        Some(exe_path)
    } else {
        None
    }
}

pub async fn pid_filename() -> PathBuf {
    match project_dir() {
        Some(path) => {
            let pidfile = path.join(PID_FILE);
            if let Some(piddir) = pidfile.parent() {
                match fs::try_exists(&piddir).await {
                    Ok(res) => {
                        if !res {
                            if let Err(e) = fs::create_dir(&piddir).await {
                                panic!("Error creating pid dir: {:?}", e);
                            }
                        }
                    }
                    Err(e) => panic!("Error checking pid dir: {:?}", e),
                };
            };

            pidfile
        }
        None => panic!("Error getting pidfile path"),
    }
}

pub fn config_filename() -> PathBuf {
    match project_dir() {
        Some(path) => path.join(CONFIG_FILE),
        None => panic!("Error getting config path"),
    }
}

pub(crate) fn database_filename() -> PathBuf {
    match project_dir() {
        Some(path) => path.join(DATABASE_FILE),
        None => panic!("Error getting database path"),
    }
}

pub async fn read_config() -> Result<String> {
    let config_filename: PathBuf = config_filename();
    let mut file = File::open(&config_filename)
        .await
        .with_context(|| format!("Error opening config file: {:?}", config_filename))?;

    let mut cfg_json = Vec::new();
    file.read_to_end(&mut cfg_json)
        .await
        .context("Failed to read config file")?;

    let cfg_json = String::from_utf8(cfg_json).context("Invalid UTF-8 sequence in config")?;
    Ok(cfg_json)
}

pub async fn write_config(cfg_json: String) -> Result<()> {
    let config_filename: PathBuf = config_filename();
    match fs::try_exists(&config_filename).await {
        Ok(true) => {
            for i in (1..=4).rev() {
                if let Ok(true) =
                    fs::try_exists(&config_filename.with_extension(format!("json.{}", i))).await
                {
                    info!("Backup .json.{}", i);
                    fs::rename(
                        &config_filename.with_extension(format!("json.{}", i)),
                        &config_filename.with_extension(format!("json.{}", i + 1)),
                    )
                    .await
                    .context(format!("Error backup config file json.{}", i))?
                }
            }
            fs::rename(&config_filename, &config_filename.with_extension("json.1"))
                .await
                .context("Error renaming config file")?
        }
        Ok(false) => {
            info!("Created config dir");
            fs::create_dir_all(config_filename.parent().unwrap())
                .await
                .context("Error creating config dir")?;
        }
        Err(e) => return Err(e.into()),
    }
    let mut file = File::create(config_filename)
        .await
        .context("Error creating config file")?;
    file.write_all(cfg_json.as_ref())
        .await
        .context("Error writing to config file")
}

pub fn parse_config(json_str: &str) -> Result<Config, serde_json::Error> {
    serde_json::from_str(json_str)
}

pub fn is_task_type_allowed(task_type: &str) -> bool {
    ALLOWED_TASK_TYPES
        .iter()
        .any(|allowed| *allowed == task_type)
}

pub fn validate_config(cfg: &Config) -> Result<(), String> {
    for stream in cfg.streams.iter() {
        if !is_task_type_allowed(&stream.task_type) {
            return Err(format!(
                "Stream {} has unsupported task_type {}",
                stream.id, stream.task_type
            ));
        }
        for output in stream.outputs.iter() {
            if !is_task_type_allowed(&output.task_type) {
                return Err(format!(
                    "Output {} of stream {} has unsupported task_type {}",
                    output.id, stream.id, output.task_type
                ));
            }
        }
    }
    Ok(())
}
