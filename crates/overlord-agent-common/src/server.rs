use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::Value;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    service::IndexerService,
    types::{ConfigUpdate, PopularHash, SearchJob},
};

struct AppState<S>
where
    S: IndexerService,
{
    service: Arc<S>,
}

impl<S> Clone for AppState<S>
where
    S: IndexerService,
{
    fn clone(&self) -> Self {
        Self {
            service: Arc::clone(&self.service),
        }
    }
}

pub struct IndexerServer<S>
where
    S: IndexerService,
{
    service: Arc<S>,
}

pub struct RunningIndexerServer {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}

impl<S> IndexerServer<S>
where
    S: IndexerService,
{
    pub fn new(service: Arc<S>) -> Self {
        Self { service }
    }

    pub async fn spawn(self, bind_addr: SocketAddr) -> Result<RunningIndexerServer> {
        let state = AppState {
            service: self.service,
        };
        let app = Router::new()
            .route("/api/internal/health", get(get_health::<S>))
            .route("/api/internal/stats", get(get_stats::<S>))
            .route("/api/internal/interfaces", get(get_interfaces::<S>))
            .route("/api/internal/search", post(post_search::<S>))
            .route("/api/internal/enrich", post(post_enrich::<S>))
            .route("/api/internal/seed-popular", post(post_seed_popular::<S>))
            .route("/api/internal/config-update", post(post_config_update::<S>))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await
                .context("indexer control server exited with error")
        });

        Ok(RunningIndexerServer {
            local_addr,
            shutdown_tx: Some(shutdown_tx),
            task,
        })
    }

    pub async fn serve(self, bind_addr: SocketAddr) -> Result<()> {
        let server = self.spawn(bind_addr).await?;
        server.wait().await
    }
}

impl RunningIndexerServer {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.task
            .await
            .context("failed to join indexer control server task")?
    }

    pub async fn wait(self) -> Result<()> {
        self.task
            .await
            .context("failed to join indexer control server task")?
    }
}

async fn get_health<S>(State(state): State<AppState<S>>) -> impl IntoResponse
where
    S: IndexerService,
{
    let payload = serde_json::json!({
        "ok": true,
        "protocol": state.service.protocol(),
        "indexer_id": state.service.indexer_id(),
        "version": state.service.version(),
    });
    (StatusCode::OK, Json(payload))
}

async fn get_stats<S>(State(state): State<AppState<S>>) -> impl IntoResponse
where
    S: IndexerService,
{
    match state.service.stats().await {
        Ok(stats) => (StatusCode::OK, Json(serde_json::json!(stats))).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn get_interfaces<S>(State(state): State<AppState<S>>) -> impl IntoResponse
where
    S: IndexerService,
{
    match state.service.interfaces().await {
        Ok(report) => (StatusCode::OK, Json(serde_json::json!(report))).into_response(),
        Err(error) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn post_search<S>(
    State(state): State<AppState<S>>,
    Json(job): Json<SearchJob>,
) -> impl IntoResponse
where
    S: IndexerService,
{
    match state.service.search(job).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn post_enrich<S>(
    State(state): State<AppState<S>>,
    Json(payload): Json<Value>,
) -> impl IntoResponse
where
    S: IndexerService,
{
    match state.service.enrich(payload).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn post_seed_popular<S>(
    State(state): State<AppState<S>>,
    Json(payload): Json<Vec<PopularHash>>,
) -> impl IntoResponse
where
    S: IndexerService,
{
    match state.service.seed_popular(payload).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn post_config_update<S>(
    State(state): State<AppState<S>>,
    Json(payload): Json<ConfigUpdate>,
) -> impl IntoResponse
where
    S: IndexerService,
{
    match state.service.apply_config(payload).await {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}
