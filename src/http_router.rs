use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use axum::Router;
use anyhow::Result;
use axum::extract::{ConnectInfo, Path, Request};
use axum::http::{header, Response, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::routing::get;
use bytes::Bytes;
use http_body_util::StreamBody;
use hyper::body::Frame;
use tokio_stream::wrappers::BroadcastStream;
use futures::StreamExt;
use tokio::io::AsyncReadExt;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::net::{TcpListener, UnixStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use crate::{database::Storage, misc};
use crate::config::HttpRouterConfig;

const ERROR_COUNT: usize = 5;

pub enum HttpRouterCmd {
    AddAlias(String, String),
    RemoveAlias(String),
    AddAllowedNet(String),
    RemoveAllowedNet(String),
    AddKey(String),
    RemoveKey(String),
}

pub struct HttpRouterTask {
    pub(crate) handle: JoinHandle<()>,
    pub(crate) cancel: CancellationToken,
    pub(crate) tx: mpsc::Sender<HttpRouterCmd>,
}

pub struct HttpRouter {
    id: u32,
    cancel: CancellationToken,
    handles: HashMap<String, JoinHandle<()>>,
    router: Router,
    url: String,
    routes: Vec<String>,
    aliases_tx: Arc<RwLock<HashMap<String, broadcast::Sender<Bytes>>>>,
    allowed_nets: Vec<String>,
    database: Arc<Storage>,
    rx: mpsc::Receiver<HttpRouterCmd>,
}

impl HttpRouter {
    pub(crate) fn new(config: HttpRouterConfig, cancel: CancellationToken, database: Arc<Storage>, rx: mpsc::Receiver<HttpRouterCmd>) -> Self {
        let router: Router = Router::new();
        let id = config.id;
        let url= config.url.clone();
        let routes = config.routes.clone();
        let allowed_nets = config.allow.clone();
        let handles = HashMap::new();
        let aliases_tx = Arc::new(RwLock::new(HashMap::new()));

        Self {
            id,
            cancel,
            handles,
            router,
            url,
            routes,
            aliases_tx,
            allowed_nets,
            database,
            rx,
        }
    }

    async fn add_alias(&mut self, alias: String, url: String) {
        self.remove_alias(alias.clone()).await;

        let (tx, _rx) = broadcast::channel::<Bytes>(1024);
        let rtx = tx.clone();
        let cancel = self.cancel.clone();
        let url = url.clone();
        let handle = tokio::spawn(async move {
            let mut error_count = 0;
            loop {
                match UnixStream::connect(&url).await {
                    Ok(mut stream) => {
                        error_count = 0;
                        let mut buffer = [0u8; 1316];
                        loop {
                            tokio::select! {
                                biased;
                                _ = cancel.cancelled() => {
                                    break;
                                },
                                res = stream.read(&mut buffer) => {
                                    match res {
                                        Ok(0) => {
                                            break;
                                        },
                                        Ok(n) => {
                                            let _ = rtx.send(Bytes::copy_from_slice(&buffer[..n]));
                                        },
                                        Err(e) => {
                                            error!("Failed to fetch remote data {:?}", e);
                                            break;
                                        },
                                    };
                                },
                            }
                        }
                    },
                    Err(e) => {
                        error_count += 1;
                        if error_count % 60 == 0 {
                            error!("Failed to connect to muxer per 60 sec: {:?}", e);
                        }
                    }
                }

                if misc::wait_interval(&cancel, 1.0).await.is_err() {
                    break;
                };
            }
        });
        self.handles.insert(alias.clone(), handle);
        self.aliases_tx.write().await.insert(alias.clone(), tx);
    }

    async fn remove_alias(&mut self, alias: String) {
        if let Some(handle) = self.handles.remove(&alias) {
            handle.abort();
        }
        let _ = self.aliases_tx.write().await.remove(&alias);
    }

    pub(crate) async fn run (&mut self) -> Result<()> {
        for (alias, socket) in self.database.aliases(&self.id)? {
            self.add_alias(alias, socket).await;
        }

        let mut app = self.router.clone();
        for route in &self.routes {
            app = app.route(
                route,
                get({
                    let aliases = Arc::clone(&self.aliases_tx);
                    let database = Arc::clone(&self.database);
                    let cancel = self.cancel.clone();
                    move |Path((key, alias)): Path<(String, String)>| {
                        async move {
                        match database.has_key(&key) {
                            Ok(true) => {},
                            Ok(false) => return (StatusCode::FORBIDDEN, "Access denied").into_response(),
                            Err(e) => {
                                error!("Failed to check HTTP access key: {:?}", e);
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }

                        let tx = {
                            let guard = aliases.read().await;
                            guard.get(&alias).cloned()
                        };

                        if tx.is_none() {
                            return (StatusCode::SERVICE_UNAVAILABLE, format!("Service unavailable {}", &alias)).into_response();
                        };

                        let rx = tx.unwrap().subscribe();
                        let stream = BroadcastStream::new(rx)
                            .take_while(move |_| {
                                let database = Arc::clone(&database);
                                let key = key.clone();
                                async move {
                                    database.has_key(&key).unwrap_or(false)
                                }
                            })
                            .filter_map(move |item| async move {
                                match item {
                                    Ok(bytes) =>  Some(Ok::<Frame<Bytes>, Infallible>(Frame::data(bytes))),
                                    Err(_) => None,
                                }
                            })
                            .take_until(async move {
                                cancel.cancelled().await;
                            });

                        let body = StreamBody::new(stream);
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(header::CONTENT_TYPE, "video/mp2t")
                            .header(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate")
                            .header(header::PRAGMA, "no-cache")
                            .header(header::EXPIRES, "0")
                            .header(header::CONNECTION, "keep-alive")
                            .body(body)
                            .unwrap()
                            .into_response()
                        }
                    }
                })
            );
        }
        app = app.fallback({
            move |uri: Uri| async move {
                (StatusCode::NOT_FOUND, format!("Not fount {}", uri)).into_response()
            }
        });

        let allowed_nets = Arc::new(self.allowed_nets.clone());
        app = app.layer(axum::middleware::from_fn(
            move |ConnectInfo(addr): ConnectInfo<SocketAddr>, req: Request, next: Next| {
                let allowed_nets = Arc::clone(&allowed_nets);
                async move {
                    if !addr.is_ipv4() || misc::is_forbidden(&addr.ip().to_string(), allowed_nets.as_ref()) {
                        return (StatusCode::FORBIDDEN, "Access denied").into_response();
                    };
                    next.run(req).await
                }
            }
        ));

        let listener = TcpListener::bind(self.url.clone()).await?;

        let cancel = self.cancel.clone();
        let server = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        ).with_graceful_shutdown(async move {
            cancel.cancelled().await;
        }).into_future();

        tokio::pin!(server);

        loop {
            tokio::select! {
                biased;
                res = &mut server => {
                    res?;
                    break;
                },
                cmd = self.rx.recv() => {
                    match cmd {
                        Some(HttpRouterCmd::AddAlias(alias, url)) => {
                            self.add_alias(alias, url).await;
                        },
                        Some(HttpRouterCmd::RemoveAlias(alias)) => {
                            self.remove_alias(alias).await;
                        },
                        None => break,
                        _ => {}
                    }
                },
            }
        }

        Ok(())
    }
}
