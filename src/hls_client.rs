use anyhow::{Result, anyhow};
use bytes::{Bytes, BytesMut};
use m3u8_rs::Playlist;
use reqwest::Client;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use url::Url;

const MAX_SEGMENTS_COUNT: usize = 50;
const CONNECTIONS_TIMEOUT_SEC: u64 = 3;
const FETCH_PLAYLIST_TIMEOUT_SEC: u64 = 3;
const FETCH_SEGMENT_TIMEOUT_SEC: u64 = 5;
const FETCH_SEGMENT_TIME_SEC: u64 = 5;
const MAX_ERROR_COUNT: usize = 3;

pub struct ChunkQueue {
    queue: VecDeque<u64>,
    capacity: usize,
}

impl ChunkQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, item: String) {
        if self.queue.len() >= self.capacity {
            self.queue.pop_back();
        }
        let hash = self.calculate_hash(&item);
        self.queue.push_front(hash);
    }

    pub fn contains(&self, item: &str) -> bool {
        let hash = self.calculate_hash(&item);
        self.queue.iter().any(|&x| x == hash)
    }

    fn calculate_hash(&self, item: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        hasher.finish()
    }
}

pub struct HlsClient {
    http_client: Client,
    master_url: Option<Url>,
    media_playlist_url: Option<Url>,
    bitrate: u64,
    segments_count: usize,
}

impl HlsClient {
    pub fn new() -> Result<Self> {
        let http_client = Client::builder()
            .redirect(reqwest::redirect::Policy::limited(5))
            .connect_timeout(Duration::from_secs(CONNECTIONS_TIMEOUT_SEC))
            .user_agent("mydemuxer")
            .build()?;
        Ok(Self {
            http_client,
            master_url: None,
            media_playlist_url: None,
            bitrate: 0,
            segments_count: 0,
        })
    }

    pub async fn get(
        &self,
        url: &str,
        cancel: CancellationToken,
    ) -> Result<(mpsc::Receiver<Bytes>, JoinHandle<()>)> {
        let master_url = Url::parse(url).map_err(|e| anyhow!(e.to_string()))?;
        let (tx, rx) = mpsc::channel(1);
        let mut downloaded_chunks = ChunkQueue::new(MAX_SEGMENTS_COUNT);
        let http_client = self.http_client.clone();

        let handle = tokio::spawn(async move {
            let body = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                _ = tx.closed() => return,
                r = fetch_url(&http_client, &master_url) => match r {
                    Ok(text) => text,
                    _ => return
                },
            };

            let media_playlist_url = match m3u8_rs::parse_playlist_res(body.as_bytes()) {
                Ok(Playlist::MasterPlaylist(master)) => {
                    if let Some(best_variant) = master.variants.iter().max_by_key(|v| v.bandwidth) {
                        match master_url.join(&best_variant.uri) {
                            Ok(u) => u,
                            Err(_) => return,
                        }
                    } else {
                        return;
                    }
                }
                Ok(Playlist::MediaPlaylist(_)) => master_url,
                Err(_) => return,
            };

            let mut is_first_fetch = true;
            let mut error_count = 0;
            let mut error_segment_count = 0;
            'segments: loop {
                let playlist_text = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    _ = tx.closed() => return,
                    r = fetch_url(&http_client, &media_playlist_url) => match r {
                        Ok(text) if !text.is_empty() => text,
                        _ if wait_interval(&cancel, &tx, 2.0).await.is_err() && error_count > MAX_ERROR_COUNT => {
                            return;
                        },
                        _ => {
                            error_count += 1;
                            continue 'segments;
                        }
                    }
                };

                let (mut segments_uris, target_duration): (Vec<(String, f32)>, f32) = {
                    match m3u8_rs::parse_playlist_res(playlist_text.as_bytes()) {
                        Ok(Playlist::MediaPlaylist(media)) => {
                            let uris: Vec<(String, f32)> = media
                                .segments
                                .iter()
                                .map(|s| (s.uri.clone(), s.duration))
                                .collect();
                            (uris, media.target_duration as f32)
                        }
                        _ if wait_interval(&cancel, &tx, 2.0).await.is_err()
                            && error_count > MAX_ERROR_COUNT =>
                        {
                            return;
                        }
                        _ => {
                            error_count += 1;
                            continue 'segments;
                        }
                    }
                };

                if is_first_fetch {
                    if segments_uris.len() > 3 {
                        let drain_start = segments_uris.len() - 3;
                        let skipped_chunks: Vec<(String, f32)> =
                            segments_uris.drain(0..drain_start).collect();
                        for (skipped_uri, _chunk_duration) in skipped_chunks {
                            downloaded_chunks.push(skipped_uri);
                        }
                    }
                    is_first_fetch = false;
                }

                for (chunk_uri, chunk_duration) in segments_uris {
                    if downloaded_chunks.contains(&chunk_uri) {
                        continue;
                    }

                    let absolute_chunk_url = match media_playlist_url.join(&chunk_uri) {
                        Ok(u) => u,
                        Err(_) => continue,
                    };

                    let response = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        _ = tx.closed() => return,
                        r = timeout(Duration::from_secs(FETCH_SEGMENT_TIMEOUT_SEC), http_client.get(absolute_chunk_url).send()) => match r {
                            Ok(Ok(response)) if response.status().is_success() => response,
                            _ if wait_interval(&cancel, &tx, 2.0).await.is_err() && error_count > MAX_ERROR_COUNT => {
                                return;
                            },
                            _ => {
                                error_count += 1;
                                continue 'segments;
                            }
                        }
                    };

                    let content_length = response.content_length().unwrap_or(0) as usize;
                    if content_length == 0 {
                        continue;
                    }
                    let mut chunk_data = BytesMut::with_capacity(content_length);

                    match timeout(Duration::from_secs(FETCH_SEGMENT_TIME_SEC), async {
                        let mut stream = response.bytes_stream();
                        while let Some(Ok(bytes)) = stream.next().await {
                            if cancel.is_cancelled() || tx.is_closed() {
                                return Err(());
                            }
                            chunk_data.extend_from_slice(&bytes);
                            if chunk_data.len() >= content_length {
                                return Ok(());
                            }
                        }
                        Err::<(), ()>(())
                    })
                    .await
                    {
                        Ok(Ok(_)) => {
                            let send_data = chunk_data.freeze();
                            if let Err(_) = tx.send(send_data).await {
                                return;
                            }
                            downloaded_chunks.push(chunk_uri);
                            error_segment_count = 0;
                            let sleep_secs = chunk_duration / 2.0;
                            if wait_interval(&cancel, &tx, sleep_secs).await.is_err() {
                                return;
                            }
                        }
                        _ => {
                            error_segment_count += 1;
                            if error_segment_count > MAX_ERROR_COUNT {
                                downloaded_chunks.push(chunk_uri);
                            }
                            continue 'segments;
                        }
                    };
                }

                let refresh_interval = if target_duration > 0.0 {
                    target_duration / 2.0
                } else {
                    2.0
                };

                if wait_interval(&cancel, &tx, refresh_interval).await.is_err() {
                    return;
                }
            }
        });

        Ok((rx, handle))
    }
}

async fn fetch_url(http_client: &Client, url: &Url) -> Result<String> {
    let response = timeout(
        Duration::from_secs(FETCH_PLAYLIST_TIMEOUT_SEC),
        http_client.get(url.clone()).send(),
    )
    .await??;
    let text = response.text().await?;

    Ok(text)
}

async fn wait_interval(
    cancel: &CancellationToken,
    tx: &mpsc::Sender<Bytes>,
    interval: f32,
) -> Result<(), ()> {
    tokio::select! {
        _ = cancel.cancelled() => Err(()),
        _ = tx.closed() => Err(()),
        _ = tokio::time::sleep(Duration::from_secs_f32(interval)) => Ok(())
    }
}
