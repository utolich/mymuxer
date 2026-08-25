use crate::config::Stream;
use crate::status::{ApiConfig, InputStats, OutputStats};
use crate::supervisor::Supervisor;
use crate::{config, misc, status};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use strum::Display;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use url::Url;

mod input_ffmpeg;
mod input_hls;
mod input_http;
mod input_rtp;
mod input_udp;
mod output_http;
mod output_rtp;
mod output_udp;

const BROADCAST_CAPACITY: usize = 256;
const BROADCAST_CHUNK_SIZE: usize = 64 * crate::packet::TS_PACKET_SIZE;

pub fn send_broadcast_chunks(tx: &broadcast::Sender<Bytes>, chunk: Bytes) -> usize {
    let len = chunk.len();

    if len <= BROADCAST_CHUNK_SIZE {
        let _ = tx.send(chunk);
        return len;
    }

    for data in chunk.chunks(BROADCAST_CHUNK_SIZE) {
        let _ = tx.send(Bytes::copy_from_slice(data));
    }

    len
}

pub struct InputTask {
    handle: JoinHandle<()>,
    cancel: CancellationToken,
    pub(crate) stats: Arc<InputStats>,
}

impl InputTask {
    pub fn new(handle: JoinHandle<()>, cancel: CancellationToken, stats: Arc<InputStats>) -> Self {
        Self {
            handle,
            cancel,
            stats,
        }
    }
}

pub struct OutputTask {
    handle: JoinHandle<()>,
    cancel: CancellationToken,
    pub(crate) stats: Arc<OutputStats>,
}

impl OutputTask {
    pub fn new(handle: JoinHandle<()>, cancel: CancellationToken, stats: Arc<OutputStats>) -> Self {
        Self {
            handle,
            cancel,
            stats,
        }
    }
}

pub struct Worker {
    pub(crate) config: config::Stream,
    pub(crate) tx: broadcast::Sender<Bytes>,
    pub(crate) remote: InputTask,
    pub(crate) outputs: HashMap<u32, OutputTask>,
    pub(crate) helper: Helper,
}

impl Worker {
    pub fn new(config: config::Stream, mut helper: Helper) -> Self {
        let (tx, _rx) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);
        //drop(_rx);

        let remote = Worker::spawn_remote(config.clone(), tx.clone(), helper.clone());
        let mut outputs = HashMap::new();

        for output in config.outputs.iter() {
            if !config::is_task_type_allowed(&output.task_type) {
                helper.log(&format!(
                    "Output task type {} is not supported",
                    output.task_type
                ));
                continue;
            }
            let task = Worker::spawn_output(output.clone(), tx.clone(), helper.clone());
            outputs.insert(output.id, task);
        }

        Self {
            config,
            tx,
            remote,
            outputs,
            helper,
        }
    }

    // ToDo
    pub fn monitor(&mut self) -> bool {
        if self.remote.handle.is_finished() {
            self.helper.log("Remote worker is finished");
            for (_, output) in self.outputs.iter() {
                output.cancel.cancel();
            }
            self.outputs = HashMap::new();
            true
        } else {
            self.outputs.retain(|_id, task| !task.handle.is_finished());
            for output in self.config.outputs.iter() {
                if !self.outputs.contains_key(&output.id) {
                    if !config::is_task_type_allowed(&output.task_type) {
                        self.helper.log(&format!(
                            "Output task type {} is not supported",
                            output.task_type
                        ));
                        continue;
                    }
                    let task =
                        Worker::spawn_output(output.clone(), self.tx.clone(), self.helper.clone());
                    self.outputs.insert(output.id, task);
                    self.helper.log(&format!("Output {} restarted", output.id));
                }
            }
            false
        }
    }

    fn spawn_remote(
        config: config::Stream,
        tx: broadcast::Sender<Bytes>,
        helper: Helper,
    ) -> InputTask {
        let rtx = tx.clone();
        let remote = config.clone();
        let helper_clone = helper.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let stats = Arc::new(InputStats::new());
        let stats_clone = Arc::clone(&stats);
        let handle = tokio::spawn(async move {
            match remote.task_type.as_str() {
                "http" => input_http::remote(rtx, remote, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                "udp" => input_udp::remote(rtx, remote, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                "rtp" => input_rtp::remote(rtx, remote, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                "hls" => input_hls::remote(rtx, remote, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                "ffmpeg" => {
                    input_ffmpeg::remote(rtx, remote, cancel_child, helper_clone, stats_clone)
                        .await
                        .unwrap()
                }
                _ => {}
            }
        });
        InputTask::new(handle, cancel, stats)
    }

    fn spawn_output(
        output: config::Output,
        tx: broadcast::Sender<Bytes>,
        helper: Helper,
    ) -> OutputTask {
        let rtx = tx.clone();
        let helper_clone = helper.clone();
        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let stats = Arc::new(OutputStats::new());
        let stats_clone = Arc::clone(&stats);
        let handle = tokio::spawn(async move {
            match output.task_type.as_str() {
                "http" => output_http::output(rtx, output, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                "udp" => output_udp::output(rtx, output, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                "rtp" => output_rtp::output(rtx, output, cancel_child, helper_clone, stats_clone)
                    .await
                    .unwrap(),
                _ => {}
            }
        });
        OutputTask::new(handle, cancel, stats)
    }

    // ToDo
    pub async fn stop(mut self) -> Result<()> {
        self.outputs.iter().for_each(|(_, task)| {
            task.cancel.cancel();
        });
        self.remote.cancel.cancel();

        let outputs = std::mem::take(&mut self.outputs);
        for (id, task) in outputs {
            match misc::wait_and_abort(task.handle).await {
                Ok(true) => self.helper.log(&format!("Output {} stopped", id)),
                Ok(false) => self.helper.log(&format!(
                    "Output {} did not stop within timeout, aborting",
                    id
                )),
                Err(e) => self.helper.log(&format!("Output {} error: {}", id, e)),
            }
        }

        let remote_handle = self.remote.handle;
        match misc::wait_and_abort(remote_handle).await {
            Ok(true) => self.helper.log("Remote stopped"),
            Ok(false) => self
                .helper
                .log("Remote did not stop within timeout, aborting"),
            Err(e) => self.helper.log(&format!("Remote error: {}", e)),
        }

        Ok(())
    }

    // pub async fn start_output(&mut self, output_id: &u32) -> Result<()> {
    //     if let Some(output) = self.config.outputs.iter().find(|output| output.id == *output_id) {
    //         if !config::is_task_type_allowed(&output.task_type) {
    //             let msg =  format!("Output task type {} is not supported", output.task_type);
    //             self.helper.log(&msg);
    //             return Err(anyhow!(msg));
    //         }
    //
    //         if self.outputs.get(output_id).is_none() {
    //             let task = Worker::spawn_output(output.clone(), self.tx.clone(), self.helper.clone());
    //             self.outputs.insert(output.id, task);
    //         }
    //     }
    //     Ok(())
    // }

    pub async fn stop_output(&mut self, output_id: &u32) -> Result<()> {
        if let Some(task) = self.outputs.remove(output_id) {
            task.cancel.cancel();
            match misc::wait_and_abort(task.handle).await {
                Ok(true) => self.helper.log(&format!("Output {} stopped", output_id)),
                Ok(false) => self.helper.log(&format!(
                    "Output {} did not stop within timeout, aborting",
                    output_id
                )),
                Err(e) => self
                    .helper
                    .log(&format!("Output {} error: {}", output_id, e)),
            }
        }
        Ok(())
    }
}

#[derive(Display, Serialize)]
pub enum LogLevels {
    #[strum(serialize = "0")]
    Info,
    #[strum(serialize = "1")]
    Warn,
    #[strum(serialize = "10")]
    Pause,
    #[strum(serialize = "11")]
    Play,
}

#[derive(Display, Serialize)]
pub enum Cmd {
    #[strum(serialize = "mark_dirty")]
    MarkDirty,
    #[strum(serialize = "unmark_dirty")]
    UnMarkDirty,
}

#[derive(Clone)]
pub struct Helper {
    worker_id: u32,
    output_id: u32,
    namespace: String,
    api_config: Option<Arc<ApiConfig>>,
    log_writer: NonBlocking,
    _log_guard: Arc<WorkerGuard>,
}

impl Helper {
    pub fn new(worker_id: u32) -> Self {
        let log_dir = match config::project_dir() {
            Some(path) => path.join(config::STREAMS_LOG_DIR),
            None => panic!("Error getting config path"),
        };

        let file_appender =
            tracing_appender::rolling::daily(log_dir, format!("s{}.log", worker_id));
        let (log_writer, log_guard) = tracing_appender::non_blocking(file_appender);

        Self {
            worker_id,
            output_id: 0,
            namespace: "main".to_string(),
            api_config: None,
            log_writer,
            _log_guard: Arc::from(log_guard),
        }
    }

    pub fn set_output_id(&mut self, output_id: u32) {
        self.output_id = output_id;
    }

    pub fn set_namespace(&mut self, namespace: &str) {
        self.namespace = namespace.to_string();
    }

    pub fn set_api_config(&mut self, api_config: ApiConfig) {
        self.api_config = Some(Arc::from(api_config));
    }

    pub fn log(&mut self, message: &str) {
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f %Z");
        let _ = writeln!(self.log_writer, "{} [{}]: {}", now, self.namespace, message);
    }

    pub fn log_info(&mut self, message: &str) {
        self.send_log(message, LogLevels::Info);
    }

    pub fn log_warn(&mut self, message: &str) {
        self.send_log(message, LogLevels::Warn);
    }

    pub fn log_pause(&mut self, message: &str) {
        self.send_log(message, LogLevels::Pause);
    }

    pub fn log_play(&mut self, message: &str) {
        self.send_log(message, LogLevels::Play);
    }

    fn send_log(&mut self, message: &str, level: LogLevels) {
        self.log(message);

        let api_config = match &self.api_config {
            Some(config) => config.clone(),
            None => return,
        };

        let msg = message.to_string();
        let level = level.to_string();
        let worker_id = self.worker_id;
        let output_id = self.output_id;

        tokio::spawn(async move {
            let mut admin_url = Url::parse(&api_config.admin_url)?;
            admin_url.set_path("/api/client");

            let api_client = api_config
                .api_client
                .as_ref()
                .ok_or_else(|| anyhow!("Api client not set"))?;

            let data = json!({
                "worker_id": worker_id,
                "output_id": output_id,
                "message": msg,
                "level": level,
            });
            let payload = status::api_data_log(&api_config, data)?;
            if let Err(e) = api_client.send(admin_url, payload).await {
                error!("{:?}", e);
            };

            Ok::<(), anyhow::Error>(())
        });
    }

    pub fn cmd_mark_dirty (&self) {
        self.send_cmd(Cmd::MarkDirty);
    }
    pub fn cmd_unmark_dirty (&self) {
        self.send_cmd(Cmd::UnMarkDirty);
    }

    fn send_cmd(&self, cmd: Cmd) {
        let api_config = match &self.api_config {
            Some(config) => config.clone(),
            None => return,
        };

        let cmd = cmd.to_string();
        let worker_id = self.worker_id;
        let output_id = self.output_id;

        tokio::spawn(async move {
            let mut admin_url = Url::parse(&api_config.admin_url)?;
            admin_url.set_path("/api/client");

            let api_client = api_config
                .api_client
                .as_ref()
                .ok_or_else(|| anyhow!("Api client not set"))?;

            let data = json!({
                "cmd": cmd,
                "worker_id": worker_id,
                "output_id": output_id,
            });
            let payload = status::api_data_cmd(&api_config, data)?;
            if let Err(e) = api_client.send(admin_url, payload).await {
                error!("{:?}", e);
            };

            Ok::<(), anyhow::Error>(())
        });
    }

}

pub struct Dump {
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

pub async fn start(supervisor: &Supervisor, id: &u32) -> Result<Worker> {
    let (stream, api_config) = {
        let config_guard = supervisor.config.read().await;
        let config = config_guard
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Config not found"))?;
        let stream = config
            .get_stream(*id)
            .cloned()
            .ok_or_else(|| anyhow!("Stream {} not found", id))?;
        let api_config = ApiConfig {
            api_client: supervisor.api_client.clone(),
            admin_url: config.mgnt.admin_url.clone(),
            hash: config.mgnt.hash.clone(),
            last_update: config.mgnt.last_update.to_string(),
        };
        (stream, api_config)
    };

    let mut helper = Helper::new(stream.id);
    helper.set_api_config(api_config);

    let worker = Worker::new(stream, helper);

    Ok(worker)
}

pub fn start_dump(config: Stream, tx: broadcast::Sender<Bytes>, helper: Helper) -> Dump {
    let task = Worker::spawn_remote(config.clone(), tx.clone(), helper.clone());

    Dump {
        handle: task.handle,
        cancel: task.cancel,
    }
}

pub async fn stop_dump(dump: Dump) -> Result<bool> {
    dump.cancel.cancel();
    misc::wait_and_abort(dump.handle).await
}
