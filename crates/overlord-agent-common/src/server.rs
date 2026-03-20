use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::Value;

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

impl<S> IndexerServer<S>
where
    S: IndexerService,
{
    pub fn new(service: Arc<S>) -> Self {
        Self { service }
    }

    pub async fn serve(self, bind_addr: SocketAddr) -> Result<()> {
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
        axum::serve(listener, app).await?;
        Ok(())
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
