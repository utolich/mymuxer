#[cfg(unix)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

extern crate core;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tracing::level_filters::LevelFilter;
use tracing::{Level, error, info};
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, Registry, fmt};

mod api_client;
mod biss;
mod config;
mod database;
mod hls_client;
mod mux;
mod packet;
mod pes;
mod probe;
mod proto;
mod psi;
mod status;
mod supervisor;
mod workers;
mod epg;
mod misc;
#[cfg(unix)]
mod http_router;

#[tokio::main]
async fn main() {
    let log_dir = match config::project_dir() {
        Some(path) => path.join(config::LOG_DIR),
        None => panic!("Error getting main log path"),
    };

    cleanup_logs(&log_dir, 7).await;
    start_cleanup_task(log_dir.clone(), 7).await;

    let log_dir_stream = match config::project_dir() {
        Some(path) => path.join(config::STREAMS_LOG_DIR),
        None => panic!("Error getting stream log path"),
    };

    cleanup_logs(&log_dir_stream, 3).await;
    start_cleanup_task(log_dir_stream.clone(), 3).await;

    let main_appender = rolling::daily(&log_dir, "main.log");
    let (main_writer, _guard_main) = non_blocking(main_appender);
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f %Z".to_string());

    let main_layer = fmt::layer()
        .with_writer(main_writer)
        .with_ansi(false)
        .with_target(true)
        .with_timer(timer)
        // from Debug to Warn
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.level() == &Level::INFO || metadata.level() == &Level::WARN
        }));

    let error_appender = rolling::daily(&log_dir, "errors.log");
    let (error_writer, _guard_error) = non_blocking(error_appender);
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.3f %Z".to_string());

    let error_layer = fmt::layer()
        .with_writer(error_writer)
        .with_ansi(false)
        .with_target(true)
        .with_timer(timer)
        .with_file(true)
        .with_line_number(true)
        // only ERROR
        .with_filter(LevelFilter::ERROR);

    Registry::default()
        .with(main_layer)
        .with(error_layer)
        .init();

    info!("MyMuxer started");

    let main_spawn = tokio::spawn(async move {
        let _ = run().await;
    });

    let _ = tokio::try_join!(main_spawn);
}

async fn run() {
    if let Err(e) = supervisor::run().await {
        error!("Supervisor failed: {:?}", e);
    }
}

async fn start_cleanup_task(log_dir: PathBuf, days: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            cleanup_logs(&log_dir, days).await;
        }
    });
}

async fn cleanup_logs(log_dir: &Path, days_to_keep: u64) {
    let now = SystemTime::now();
    let max_age = Duration::from_secs(days_to_keep * 24 * 60 * 60);

    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();

            if path.is_file() {
                if let Ok(metadata) = fs::metadata(&path) {
                    if let Ok(modified) = metadata.modified() {
                        if let Ok(age) = now.duration_since(modified) {
                            if age > max_age {
                                if let Err(e) = fs::remove_file(&path) {
                                    error!("Remove failed {:?}: {}", path, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
