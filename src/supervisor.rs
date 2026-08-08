use crate::api_client::ApiClient;
use crate::config::{Config, Stream};
use crate::probe::{ProbeResult, probe};
use crate::status::{ApiConfig, OutputStats};
use crate::workers::{Dump, Worker};
use crate::{config, epg, status, workers};
use anyhow::{Result, anyhow};
use axum::body::Body;
use axum::extract::Path;
use axum::http::{Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::{
    Json, Router,
    extract::{ConnectInfo, Request},
    routing::{get, post},
};
use bytes::Bytes;
use chrono::Utc;
use hyper::StatusCode;
use ipnet::Ipv4Net;
use scopeguard::guard;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::string::String;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use strum::Display;
use tokio::net::TcpListener;
use tokio::sync::{RwLock, broadcast};
use tokio::time::{sleep, timeout};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use url::Url;
use crate::epg::epg::EpgTask;

const URL_LISTENER: &str = "0.0.0.0:8787";
const SAVE_PERIOD_SEC: i64 = 900;

#[derive(Serialize)]
struct ResponseJson {
    success: bool,
    message: String,
    id: u32,
}

#[derive(Display, Serialize)]
enum EventsType {
    #[strum(serialize = "on_start_stream")]
    OnStartStream,
    #[strum(serialize = "on_stop_stream")]
    OnStopStream,
    #[strum(serialize = "on_start_output")]
    OnStartOutput,
    #[strum(serialize = "on_stop_output")]
    OnStopOutput,
    #[strum(serialize = "on_error")]
    OnError,
    #[strum(serialize = "on_probe")]
    OnProbe,
    #[strum(serialize = "on_save_config")]
    OnSaveConfig,
    #[strum(serialize = "on_load_config")]
    OnLoadConfig,
}

#[derive(Serialize)]
struct EventResponse {
    event: String,
    success: bool,
    message: String,
    result: Value,
}

impl EventResponse {
    fn new(event: EventsType) -> Self {
        Self {
            event: event.to_string(),
            success: true,
            message: "".to_string(),
            result: Value::Null,
        }
    }

    pub fn success(&mut self, message: String) -> &mut Self {
        self.success = true;
        self.message = message;
        self
    }

    pub fn failed(&mut self, message: String) -> &mut Self {
        self.success = false;
        self.message = message;
        self
    }

    pub fn result(&mut self, result: Value) -> &mut Self {
        self.result = result;
        self
    }
}

pub struct Supervisor {
    pub(crate) src_config: RwLock<Option<String>>,
    pub(crate) config: RwLock<Option<Config>>,
    dirty: AtomicBool,
    last_update: AtomicI64,
    workers: RwLock<HashMap<u32, Worker>>,
    workers_lock: Mutex<HashSet<u32>>,
    probe_lock: Mutex<HashSet<u32>>,
    dumps: RwLock<HashMap<u32, Dump>>,
    pub(crate) api_client: Option<ApiClient>,
    epg: RwLock<HashMap<u32, EpgTask>>
}

impl Supervisor {
    fn new() -> Self {
        Self {
            src_config: RwLock::new(None),
            config: RwLock::new(None),
            dirty: AtomicBool::new(false),
            last_update: AtomicI64::new(0),
            workers: RwLock::new(HashMap::new()),
            workers_lock: Mutex::new(HashSet::new()),
            probe_lock: Mutex::new(HashSet::new()),
            dumps: RwLock::new(HashMap::new()),
            api_client: ApiClient::new(Duration::from_secs(3)).ok(),
            epg: RwLock::new(HashMap::new())
        }
    }

    async fn get_config(&self) -> String {
        let src_config = {
            let guard_src_config = self.src_config.read().await;
            guard_src_config.clone()
        };
        src_config.unwrap_or_else(|| "".to_string())
    }

    async fn list(&self) -> serde_json::Result<String> {
        let mut list = HashSet::new();
        let guard = self.workers.read().await;
        for (id, worker) in guard.iter() {
            list.insert(Self::worker_stat_value(id, worker));
        }
        let data = serde_json::to_value(list)?;
        Ok(serde_json::to_string(&data)?)
    }

    fn worker_stat_value(id: &u32, worker: &Worker) -> Value {
        let mut outputs = HashSet::new();

        for (p, task) in worker.outputs.iter() {
            let stats: Arc<OutputStats> = Arc::clone(&task.stats);
            let output_stats = json!({
                "output_id": p,
                "output_stats": *stats
            });
            outputs.insert(output_stats);
        }
        json!({
            "timestamp": Utc::now().timestamp(),
            "stream_id": id,
            "stream_name": worker.config.name,
            "input_stats": *worker.remote.stats,
            "outputs": outputs,
        })
    }

    async fn worker_stat(&self, id: &u32) -> Option<Value> {
        let guard = self.workers.read().await;
        guard
            .get(id)
            .map(|worker| Self::worker_stat_value(id, worker))
    }

    async fn load(&self, cfg_json: String) -> Result<()> {
        let parsed_config = match config::parse_config(&cfg_json) {
            Ok(cfg) => {
                if let Err(e) = config::validate_config(&cfg) {
                    error!("Error in config validation: {}", e);
                    return Err(anyhow!(e));
                }
                info!("{:?}", &cfg);
                cfg
            }
            Err(e) => {
                let msg = format!("Error in parse config: {:?}", e);
                error!(msg);
                return Err(anyhow!(msg));
            }
        };

        let update_result = timeout(Duration::from_millis(50), async {
            let mut src_lock = self.src_config.write().await;
            let mut cfg_lock = self.config.write().await;

            *src_lock = Some(cfg_json);
            *cfg_lock = Some(parsed_config);

            Ok::<(), anyhow::Error>(())
        })
        .await;

        if let Err(_) = update_result {
            self.on_error("Config update timed out".into()).await;
            return Err(anyhow!("Lock contention"));
        }

        Ok(())
    }

    async fn dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
        self.last_update
            .store(Utc::now().timestamp(), Ordering::Relaxed);
    }

    async fn save(&self) -> Result<()> {
        let src_config = {
            let src_config_guard = self.src_config.read().await;
            src_config_guard
                .as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("Config not loaded"))?
        };
        config::write_config(src_config).await?;
        self.dirty.store(false, Ordering::Relaxed);
        info!("Config saved");

        Ok(())
    }

    async fn startall_worker(&self) -> Result<()> {
        let ids: Vec<u32> = {
            let config_guard = self.config.read().await;
            if let Some(config) = &*config_guard {
                config.streams.iter().map(|stream| stream.id).collect()
            } else {
                return Ok(());
            }
        };

        for id in ids {
            if let Err(e) = self.start_worker(&id).await {
                error!("Error start worker {}: {:?}", id, e);
            }
        }

        Ok(())
    }

    async fn start_worker(&self, id: &u32) -> Result<()> {
        if self.workers.read().await.contains_key(id) {
            return Ok(());
        }

        {
            let mut lock = self.workers_lock.lock().unwrap();

            if lock.contains(id) {
                return Ok(());
            }

            lock.insert(*id);
        }

        let _guard = guard(id, |id| {
            if let Ok(mut lock) = self.workers_lock.lock() {
                lock.remove(id);
            }
        });

        let worker = workers::start(self, id).await?;
        self.workers.write().await.insert(*id, worker);

        Ok(())
    }

    async fn stopall_worker(&self) -> Result<()> {
        let ids: Vec<u32> = {
            let config_guard = self.config.read().await;
            if let Some(config) = &*config_guard {
                config.streams.iter().map(|stream| stream.id).collect()
            } else {
                return Ok(());
            }
        };

        for id in ids {
            if let Err(e) = self.stop_worker(&id).await {
                error!("Error stop worker {}: {:?}", id, e);
            }
        }

        Ok(())
    }

    async fn stop_worker(&self, id: &u32) -> Result<()> {
        if !self.workers.read().await.contains_key(id) {
            return Ok(());
        }

        {
            let mut lock = self.workers_lock.lock().unwrap();

            if lock.contains(id) {
                return Ok(());
            }

            lock.insert(*id);
        }

        let _guard = guard(id, |id| {
            if let Ok(mut lock) = self.workers_lock.lock() {
                lock.remove(id);
            }
        });

        let worker = {
            let mut guard_workers = self.workers.write().await;
            guard_workers.remove(id)
        }
        .ok_or_else(|| anyhow!("Worker {} not found", id))?;

        worker.stop().await?;

        Ok(())
    }

    // async fn start_output(&self, id: &u32, output_id: &u32) -> Result<()> {
    //     let mut worker = {
    //         let guard = self.workers.read().await;
    //         guard.get(id).cloned().ok_or_else(|| anyhow!("Worker {} not found", id))?
    //     };
    //
    //     worker.start_output(output_id).await?;
    //
    //     Ok(())
    // }
    //
    // async fn stop_output(&self, id: &u32, output_id: &u32) -> Result<()> {
    //     let mut worker = {
    //         let guard = self.workers.read().await;
    //         guard.get(id).cloned().ok_or_else(|| anyhow!("Worker {} not found", id))?
    //     };
    //
    //     worker.stop_output(output_id).await?;
    //
    //     Ok(())
    // }

    async fn restart_worker(&self, id: &u32) -> Result<()> {
        {
            let mut lock = self.workers_lock.lock().unwrap();

            if lock.contains(id) {
                return Ok(());
            }

            lock.insert(*id);
        }

        let _guard = guard(id, |id| {
            if let Ok(mut lock) = self.workers_lock.lock() {
                lock.remove(id);
            }
        });

        let worker = {
            let mut guard_workers = self.workers.write().await;
            guard_workers.remove(id)
        }
        .ok_or_else(|| anyhow!("Worker {} not found", id))?;

        worker.stop().await?;
        sleep(Duration::from_secs(5)).await;
        let worker = workers::start(self, id).await?;
        self.workers.write().await.insert(*id, worker);

        Ok(())
    }

    async fn monitor(self: Arc<Self>) -> Result<()> {
        info!(
            "Monitor started. Autosave config every {} sec.",
            SAVE_PERIOD_SEC
        );
        loop {
            let dirty = self.dirty.load(Ordering::Relaxed);
            let last_update = self.last_update.load(Ordering::Relaxed);

            if dirty && (Utc::now().timestamp() - last_update) > SAVE_PERIOD_SEC {
                match self.save().await {
                    Ok(_) => info!("Autosave config successfully"),
                    Err(e) => error!("Error in save config: {:?}", e),
                }
            }

            let mut restart_ids: Vec<u32> = Vec::new();
            {
                let mut workers = self.workers.write().await;
                workers.retain(|id, worker| {
                    let finished = worker.monitor();
                    if finished {
                        restart_ids.push(*id);
                    }
                    !finished
                });
            }
            for id in restart_ids {
                let supervisor = Arc::clone(&self);
                tokio::spawn(async move {
                    if let Err(e) = supervisor.restart_worker(&id).await {
                        error!("Error restart worker {}: {:?}", id, e);
                    }
                });
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn on_error(&self, msg: String) {
        let mut event_response = EventResponse::new(EventsType::OnError);
        event_response.failed(msg);
        if let Err(e) = self.event(event_response).await {
            error!("{:?}", e);
        };
    }

    async fn status(&self) -> Result<()> {
        let mut err_count = 0;
        let mut err_count_payload = 0;
        let mut err_count_send = 0;
        let err_timer_duration: i32 = 5;
        let err_count_per_min = 60_i32.saturating_div(err_timer_duration);
        let mut timer = tokio::time::interval(Duration::from_secs(err_timer_duration as u64));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let api_client = self
            .api_client
            .as_ref()
            .ok_or_else(|| anyhow!("API client not set"))?;

        loop {
            tokio::select! {
                _ = timer.tick() => {
                    let config = {
                        let guard = self.config.read().await;
                        guard.as_ref().cloned()
                    };

                    match config {
                        Some(config) => {
                            let worker_ids: Vec<u32> = {
                                let guard = self.workers.read().await;
                                guard.keys().copied().collect()
                            };

                            let top = match status::shell_top().await {
                                Some(top) => String::from_utf8_lossy(&top.stdout).to_string(),
                                None => "Error".to_string()
                            };

                            let memory = status::memory_stats();

                            let data = json!({
                                "top": top,
                                "memory": memory,
                                "worker_ids": worker_ids
                            });

                            let api_config = ApiConfig {
                                api_client: None,
                                admin_url: config.mgnt.admin_url.clone(),
                                hash: config.mgnt.hash.clone(),
                                last_update: config.mgnt.last_update.to_string()
                            };

                            let payload = match status::api_data_status(&api_config, data) {
                                Ok(payload) => payload,
                                Err(e) => {
                                    if err_count_payload == 0 {
                                        error!("Error in status serialization: {:?}", e);
                                    } else if err_count_payload >= err_count_per_min {
                                        error!("Error occurred {} times per 1 min", err_count_payload);
                                        err_count_payload = 0;
                                    }
                                    err_count_payload += 1;
                                    continue;
                                }
                            };

                            let mut admin_url = Url::parse(&config.mgnt.admin_url)?;
                            admin_url.set_path("/api/client");
                            if let Err(e) = api_client.send(admin_url, payload).await {
                                if err_count_send == 0 {
                                    error!("{:?}", e);
                                } else if err_count_send >= err_count_per_min {
                                    error!("Error occurred {} times per 1 min", err_count_send);
                                    err_count_send = 0;
                                }
                                err_count_send += 1;
                            };
                        },
                        None => {
                           if err_count == 0 || err_count >= err_count_per_min {
                                error!("Config not loaded");
                           }
                           err_count += 1;
                        }
                    }
                }
            }
        }
    }
    async fn event(&self, event_response: EventResponse) -> Result<()> {
        let api_client = self
            .api_client
            .as_ref()
            .ok_or_else(|| anyhow!("API client not set"))?;

        let config = {
            let guard = self.config.read().await;
            guard.as_ref().cloned()
        };

        match config {
            Some(config) => {
                let api_config = ApiConfig {
                    api_client: None,
                    admin_url: config.mgnt.admin_url.clone(),
                    hash: config.mgnt.hash.clone(),
                    last_update: config.mgnt.last_update.to_string(),
                };
                match status::api_data_event(&api_config, serde_json::to_value(event_response)?) {
                    Ok(payload) => {
                        let mut admin_url = Url::parse(&api_config.admin_url)?;
                        admin_url.set_path("/api/client");
                        if let Err(e) = api_client.send(admin_url, payload).await {
                            error!("Failed to send event: {:?}", e);
                        };
                    }
                    Err(e) => {
                        error!("Error in event serialization: {:?}", e);
                    }
                }
            }
            None => {}
        }
        Ok(())
    }

    async fn probe(&self, id: &u32) -> Result<ProbeResult> {
        {
            let mut lock = self.probe_lock.lock().unwrap();
            if lock.contains(id) {
                return Err(anyhow!("Probe for worker with id {} in process", id));
            }
            lock.insert(*id);
        }

        let _guard = guard(id, |id| {
            if let Ok(mut lock) = self.probe_lock.lock() {
                lock.remove(id);
            }
        });

        let probe_input = {
            let guard = self.workers.read().await;
            guard
                .get(id)
                .map(|worker| (worker.config.id, worker.tx.subscribe()))
        };

        match probe_input {
            Some((stream_id, rx)) => {
                let probe_info = probe(stream_id, rx).await;
                Ok(probe_info)
            }
            None => {
                error!("Worker with id {} not found", id);
                Err(anyhow!("Worker with id {} not found", id))
            }
        }
    }

    async fn start_dump(&self, id: &u32, tx: broadcast::Sender<Bytes>) -> Result<u32> {
        let config_helper = {
            let guard_workers = self.workers.read().await;
            guard_workers.values().find_map(|worker| {
                worker.outputs.get(id)?;
                let mut helper = worker.helper.clone();
                helper.set_output_id(*id);
                match worker.config.outputs.iter().find(|output| output.id == *id) {
                    Some(output) => {
                        let schema = match output.task_type.as_str() {
                            "http" => "http",
                            "udp" => "udp",
                            "rtp" => "rtp",
                            _ => return None,
                        };
                        let config = Stream {
                            id: worker.config.id,
                            remote_url: format!("{}://{}", schema, output.output_url.clone()),
                            biss_key: output.biss_key.clone(),
                            name: format!("{}_dump", output.name.clone()),
                            task_type: output.task_type.clone(),
                            outputs: vec![],
                            err_count: 0,
                        };
                        Some((config, helper))
                    }
                    None => return None,
                }
            })
        };
        match config_helper {
            Some((config, helper)) => {
                let dump = workers::start_dump(config, tx, helper);
                self.dumps.write().await.insert(*id, dump);

                Ok(*id)
            }
            None => Err(anyhow!("Output id {} not found", id)),
        }
    }

    async fn stop_dump(&self, id: &u32) -> Result<()> {
        let dump = {
            let mut guard_dump = self.dumps.write().await;
            guard_dump.remove(&id)
        };
        if let Some(dump) = dump {
            let _ = workers::stop_dump(dump).await;
            Ok(())
        } else {
            Err(anyhow!("Dump with id {} not found", id))
        }
    }

    async fn forbidden(&self, ip: String) -> bool {
        let config = self.config.read().await;
        match config.as_ref() {
            Some(config) => {
                let mgnt = &config.mgnt;
                let mut deny = true;
                if mgnt.allow.len() == 0 {
                    deny = false;
                } else {
                    let ip: Ipv4Addr = ip.parse().unwrap();
                    for net in mgnt.allow.clone() {
                        let ipnet: Ipv4Net = net.parse().unwrap();
                        if ipnet.contains(&ip) {
                            deny = false;
                            break;
                        }
                    }
                }
                deny
            }
            None => false,
        }
    }

    async fn epg_start(&self, id: &u32) -> Result<()>{
        let mut epg_task = self.epg.write().await;

        let (start_config, api_config) = {
            let guard = self.config.read().await;
            let config = guard
                .as_ref()
                .cloned()
                .ok_or_else(|| anyhow!("Config not loaded"))?;

            let epg_config = config.epg_configs
                .iter()
                .find_map(|c| if c.id == *id { Some(c) } else { None })
                .ok_or_else(|| anyhow!("EPG config with id {} not found", id))?;

            (epg_config.clone(), ApiConfig {
                api_client: self.api_client.clone(),
                admin_url: config.mgnt.admin_url.clone(),
                hash: config.mgnt.hash.clone(),
                last_update: config.mgnt.last_update.to_string(),
            })
        };

        let cancel = CancellationToken::new();
        let cancel_child = cancel.child_token();
        let handle = tokio::spawn(async move {
            if let Err(e) = epg::epg::run(cancel_child, start_config, api_config).await {
                error!("EPG failed: {:?}", e);
            }
        });

        epg_task.insert(*id, EpgTask{
            handle,
            cancel
        });

        Ok(())
    }

    async fn epg_stop(&self, id: &u32) -> Result<()>{
        let epg_task = {
            self.epg.write().await.remove(&id)
        };

        match epg_task {
            Some(epg_task) => {
                epg_task.cancel.cancel();
                Worker::wait_and_abort(epg_task.handle).await?;
            },
            None => return Err(anyhow!("EPG not running"))
        }

        Ok(())
    }
}

pub async fn run() -> Result<()> {
    let supervisor = Arc::new(Supervisor::new());
    let _ = config::pid_filename().await;
    match config::read_config().await {
        Ok(cfg_json) => {
            supervisor.load(cfg_json).await?;
        }
        Err(e) => {
            error!("Error in read config: {:?}", e);
        }
    }

    let monitor = Arc::clone(&supervisor);
    tokio::spawn(async move {
        let _ = monitor.monitor().await;
    });

    let status = Arc::clone(&supervisor);
    tokio::spawn(async move {
        let _ = status.status().await;
    });

    let mut err_count = 0;
    loop {
        let supervisor = Arc::clone(&supervisor);
        tokio::select! {
            res = server(supervisor) => {
                match res {
                    Ok(_) => {
                        info!("Supervisor server started");
                    },
                    Err(e) => {
                        info!("Supervisor failed: {:?}", e);
                        if err_count == 5 {
                            break;
                        }
                        err_count += 1;
                    }
                }
            }
        }
        tokio::select! {
            _ = sleep(Duration::from_secs(10)) => {}
        }
    }

    Ok(())
}

async fn server(supervisor: Arc<Supervisor>) -> Result<()> {
    let app: Router = Router::new()
        .route(
            "/",
            get({
                move || async move {
                    Json(ResponseJson {
                        success: true,
                        message: "myMuxer ready!".to_string(),
                        id: 0,
                    })
                }
            }),
        )
        .route(
            "/epg/start/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    tokio::spawn(async move {
                        match supervisor.epg_start(&id).await {
                            Ok(_) => {},
                            Err(e) => error!("{:?}", e)
                        }
                    });
                    Json(json!({
                        "success": true
                    }))
                }
            }),
        )
        .route(
            "/epg/stop/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    tokio::spawn(async move {
                        match supervisor.epg_stop(&id).await {
                            Ok(_) => {},
                            Err(e) => error!("{:?}", e)
                        }
                    });
                    Json(json!({
                        "success": true
                    }))
                }
            }),
        )
        .route(
            "/probe/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    tokio::spawn(async move {
                        let mut event_response = EventResponse::new(EventsType::OnProbe);
                        match supervisor.probe(&id).await {
                            Ok(probe_info) => {
                                event_response
                                    .success(format!("s{} Probe for stream successfully", id))
                                    .result(serde_json::to_value(probe_info).unwrap());
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                            Err(e) => {
                                event_response
                                    .failed(format!("s{} Failed to probe for stream: {:?}", id, e));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                        }
                    });
                    Json(ResponseJson {
                        success: true,
                        message: format!("Worker with id {} probing started", &id),
                        id,
                    })
                }
            }),
        )
        .route(
            "/startdump/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    let (tx, _rx) = broadcast::channel::<Bytes>(256);
                    //drop(_rx);

                    let rx = tx.subscribe();
                    match supervisor.start_dump(&id, tx).await {
                        Ok(id) => {
                            let guard = guard(id, |id| {
                                tokio::spawn(async move {
                                    let _ = supervisor.stop_dump(&id).await;
                                });
                            });

                            let stream = BroadcastStream::new(rx).map(move |res| {
                                let _guard = &guard;
                                match res {
                                    Ok(bytes) => Ok(bytes),
                                    Err(_) => Err(std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        "Lagged",
                                    )),
                                }
                            });

                            Body::from_stream(stream).into_response()
                        }
                        Err(e) => (
                            StatusCode::NOT_FOUND,
                            format!("Failed to create dump for p{}: {:?}", &id, e),
                        )
                            .into_response(),
                    }
                }
            }),
        )
        .route(
            "/stopdump/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    match supervisor.stop_dump(&id).await {
                        Ok(()) => {
                            (StatusCode::OK, format!("Dump for p{} stopped", &id)).into_response()
                        }
                        Err(e) => (
                            StatusCode::NOT_FOUND,
                            format!("Failed to stop dump for p{}: {:?}", &id, e),
                        )
                            .into_response(),
                    }
                }
            }),
        )
        .route(
            "/load",
            post({
                let supervisor: Arc<Supervisor> = Arc::clone(&supervisor);
                move |cfg_json: String| async move {
                    tokio::spawn(async move {
                        let mut event_response = EventResponse::new(EventsType::OnLoadConfig);
                        match supervisor.load(cfg_json).await {
                            Ok(_) => {
                                supervisor.dirty().await;
                                event_response.success("Config loaded successfully".to_string());
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                            Err(e) => {
                                event_response.failed(format!("Error in load config: {:?}", e));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                        }
                    });
                    Json(ResponseJson {
                        success: true,
                        message: "Config loading".to_string(),
                        id: 0,
                    })
                }
            }),
        )
        .route(
            "/save",
            get({
                let supervisor = Arc::clone(&supervisor);
                move || async move {
                    tokio::spawn(async move {
                        let mut event_response = EventResponse::new(EventsType::OnSaveConfig);
                        match supervisor.save().await {
                            Ok(_) => {
                                event_response.success("Config saved successfully".to_string());
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                            Err(e) => {
                                event_response.failed(format!("Error in save config: {:?}", e));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                        }
                    });
                    Json(ResponseJson {
                        success: true,
                        message: "Config saving progress".to_string(),
                        id: 0,
                    })
                }
            }),
        )
        .route(
            "/config",
            get({
                let supervisor = Arc::clone(&supervisor);
                move || async move {
                    let body = supervisor.get_config().await;
                    Response::builder()
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(body)
                        .unwrap()
                }
            }),
        )
        .route(
            "/list",
            get({
                let supervisor = Arc::clone(&supervisor);
                move || async move {
                    let body = supervisor.list().await.unwrap();
                    Response::builder()
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(body)
                        .unwrap()
                }
            }),
        )
        .route(
            "/stats/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    let data = supervisor.worker_stat(&id).await;
                    let body = serde_json::to_string(&data)
                        .unwrap_or_else(|e| format!("Error retrieving statistics: {:?}", e));
                    Response::builder()
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(body)
                        .unwrap()
                }
            }),
        )
        .route(
            "/startall",
            get({
                let supervisor = Arc::clone(&supervisor);
                move || async move {
                    tokio::spawn(async move {
                        let _ = match supervisor.startall_worker().await {
                            Ok(_) => {
                                info!("All workers started");
                            }
                            Err(e) => {
                                info!("Error in start all workers: {:?}", e);
                            }
                        };
                    });

                    Json(ResponseJson {
                        success: true,
                        message: "All workers starting".to_string(),
                        id: 0,
                    })
                }
            }),
        )
        .route(
            "/stopall",
            get({
                let supervisor = Arc::clone(&supervisor);
                move || async move {
                    tokio::spawn(async move {
                        match supervisor.stopall_worker().await {
                            Ok(_) => {
                                info!("All workers stopped");
                            }
                            Err(e) => {
                                info!("Error in stop all workers: {:?}", e);
                            }
                        }
                    });
                    Json(ResponseJson {
                        success: true,
                        message: "All workers stopping".to_string(),
                        id: 0,
                    })
                }
            }),
        )
        .route(
            "/start/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    tokio::spawn(async move {
                        let mut event_response = EventResponse::new(EventsType::OnStartStream);
                        match supervisor.start_worker(&id).await {
                            Ok(_) => {
                                event_response
                                    .success(format!("s{} Stream started successfully", id));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                            Err(e) => {
                                event_response
                                    .failed(format!("s{} Failed to start the stream: {:?}", id, e));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                        }
                    });
                    Json(ResponseJson {
                        success: true,
                        message: format!("Worker with id {} started", &id),
                        id,
                    })
                }
            }),
        )
        .route(
            "/stop/{id}",
            get({
                let supervisor = Arc::clone(&supervisor);
                move |Path(id): Path<u32>| async move {
                    tokio::spawn(async move {
                        let mut event_response = EventResponse::new(EventsType::OnStopStream);
                        match supervisor.stop_worker(&id).await {
                            Ok(_) => {
                                event_response
                                    .success(format!("s{} Stream stopped successfully", id));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                            Err(e) => {
                                event_response
                                    .failed(format!("s{} Failed to stop the stream; {:?}", id, e));
                                if let Err(e) = supervisor.event(event_response).await {
                                    error!("{:?}", e);
                                };
                            }
                        }
                    });
                    Json(ResponseJson {
                        success: true,
                        message: format!("Worker with id {} stopped", &id),
                        id,
                    })
                }
            }),
        )
        // .route(
        //     "/start/{id}/task/{task_id}",
        //     get({
        //         let supervisor = supervisor.clone();
        //         move |Path((id, task_id)): Path<(u32, u32)>| async move {
        //             let mut event_response = EventResponse::new(EventsType::OnStartOutput);
        //             tokio::spawn(async move {
        //                 match supervisor.start_output(&id, &task_id).await {
        //                     Ok(_) => {
        //                         event_response
        //                             .success(format!("s{}p{} Output started successfully", id, task_id));
        //                         if let Err(e) = supervisor.event(event_response).await {
        //                             error!("{:?}", e);
        //                         };
        //                     },
        //                     Err(e) => {
        //                         event_response
        //                             .failed(format!("s{}p{} Failed to start the output", id, task_id));
        //                         if let Err(e) = supervisor.event(event_response).await {
        //                             error!("{:?}", e);
        //                         };
        //
        //                     }
        //                 }
        //             });
        //             Json(ResponseJson {
        //                 success: true,
        //                 message: format!("Task with id {}:{} started", &id, &task_id),
        //                 id
        //             })
        //         }
        //     }),
        // )
        // .route(
        //     "/stop/{id}/task/{task_id}", get({
        //         let supervisor = supervisor.clone();
        //         move |Path((id, task_id)): Path<(u32, u32)>| async move {
        //             let mut event_response = EventResponse::new(EventsType::OnStopOutput);
        //             tokio::spawn(async move {
        //                 match supervisor.stop_output(&id, &task_id).await {
        //                     Ok(_) => {
        //                         event_response
        //                             .success(format!("s{}p{} Output stopped successfully", id, task_id));
        //                         if let Err(e) = supervisor.event(event_response).await {
        //                             error!("{:?}", e);
        //                         };
        //                     },
        //                     Err(e) => {
        //                         event_response
        //                             .failed(format!("s{}p{} Failed to stop the output", id, task_id));
        //                         if let Err(e) = supervisor.event(event_response).await {
        //                             error!("{:?}", e);
        //                         };
        //
        //                     }
        //                 }
        //             });
        //             Json(ResponseJson {
        //                 success: true,
        //                 message: format!("Task with id {}:{} stopped", &id, &task_id),
        //                 id
        //             })
        //         }
        //     }),
        // )
        // .route(
        //     "/restart",
        //     get({
        //         let supervisor = Arc::clone(&supervisor);
        //         move |Query(id): Query<u32>| async move {
        //             tokio::spawn(async move {
        //                 match supervisor.restart_worker(&id).await {
        //                     Ok(_) => {},
        //                     Err(e) => {}
        //                 }
        //             });
        //             Json(ResponseJson {
        //                 success: true,
        //                 message: format!("Worker with id {} restarted", &id),
        //                 id
        //             })
        //         }
        //     }),
        // )
        .fallback({
            move |uri: Uri| async move {
                Json(ResponseJson {
                    success: false,
                    message: format!("No route for uri={}", &uri),
                    id: 0,
                })
            }
        })
        .layer(axum::middleware::from_fn(
            move |ConnectInfo(addr): ConnectInfo<SocketAddr>, req: Request, next: Next| {
                let supervisor = Arc::clone(&supervisor);
                async move {
                    if addr.is_ipv4() {
                        match supervisor.forbidden(addr.ip().to_string()).await {
                            true => (StatusCode::FORBIDDEN, format!("Access denied {}", addr))
                                .into_response(),
                            false => next.run(req).await,
                        }
                    } else {
                        (StatusCode::FORBIDDEN, "Access denied").into_response()
                    }
                }
            },
        ));
    // .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind(URL_LISTENER).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
