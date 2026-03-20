use anyhow::{Context, Result};
use reqwest::Url;

use crate::types::{
    AgentInterfaceReport, ConfigUpdate, IndexerRegistration, IndexerStats, PopularHash,
    RegisterRequest, RegistrationResponse, ResultBatch, SearchJob, SnoopEntry,
};

#[derive(Clone)]
pub struct CoordinatorClient {
    base_url: Url,
    http: reqwest::Client,
}

impl CoordinatorClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self {
            base_url: Url::parse(base_url)
                .with_context(|| format!("invalid coordinator base url: {base_url}"))?,
            http: reqwest::Client::new(),
        })
    }

    pub async fn register(&self, payload: &RegisterRequest) -> Result<IndexerRegistration> {
        let url = self.base_url.join("/api/internal/register")?;
        let response = self
            .http
            .post(url)
            .json(payload)
            .send()
            .await?
            .error_for_status()?
            .json::<RegistrationResponse>()
            .await?;
        Ok(response.registered)
    }

    pub async fn post_results(&self, batch: &ResultBatch) -> Result<()> {
        let url = self.base_url.join("/api/internal/results")?;
        self.http
            .post(url)
            .json(batch)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn dispatch_search(&self, job: &SearchJob) -> Result<()> {
        let url = self.base_url.join("/api/search")?;
        self.http
            .post(url)
            .json(job)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn flush_snoop(&self, indexer_id: uuid::Uuid, entries: &[SnoopEntry]) -> Result<()> {
        let url = self.base_url.join("/api/internal/snoop-flush")?;
        self.http
            .post(url)
            .json(&serde_json::json!({
                "indexer_id": indexer_id,
                "entries": entries,
            }))
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn restore_snoop(&self, indexer_id: uuid::Uuid) -> Result<Vec<SnoopEntry>> {
        let url = self
            .base_url
            .join(&format!("/api/internal/snoop-restore/{indexer_id}"))?;
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<SnoopEntry>>()
            .await?)
    }

    pub async fn popular_hashes(&self) -> Result<Vec<PopularHash>> {
        let url = self.base_url.join("/api/internal/popular-hashes")?;
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<PopularHash>>()
            .await?)
    }

    pub async fn stats(&self) -> Result<IndexerStats> {
        let url = self.base_url.join("/api/internal/stats")?;
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<IndexerStats>()
            .await?)
    }

    pub async fn interfaces(&self) -> Result<AgentInterfaceReport> {
        let url = self.base_url.join("/api/internal/interfaces")?;
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<AgentInterfaceReport>()
            .await?)
    }

    pub async fn apply_config_update(&self, payload: &ConfigUpdate) -> Result<()> {
        let url = self.base_url.join("/api/internal/config-update")?;
        self.http
            .post(url)
            .json(payload)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
}
