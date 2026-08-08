use std::time::Duration;

use anyhow::{Result, anyhow};
use reqwest::{Client, StatusCode, header};
use url::Url;

pub struct ApiResponse {
    pub(crate) status: StatusCode,
    pub(crate) body: String,
}

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
}

impl ApiClient {
    pub fn new(timeout: Duration) -> Result<Self> {
        let client = Client::builder().timeout(timeout).build()?;
        Ok(Self { client })
    }

    /// Sends `payload` as JSON (string body) to `admin_url`.
    ///
    /// Returns `ApiResponse` even if the HTTP status is non-2xx; errors are reserved
    /// for transport-level failures.
    pub async fn send(&self, admin_url: Url, payload: String) -> Result<ApiResponse> {
        let resp = self
            .client
            .post(admin_url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(payload)
            .send()
            .await
            .map_err(|e| anyhow!("Error in send status: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("Failed to read response body: {e}"))?;

        if status.is_success() {
            Ok(ApiResponse { status, body })
        } else {
            Err(anyhow!("API send failed: {} {}", status, body))
        }
    }

    pub async fn get(&self, admin_url: Url) -> Result<String> {
        let resp = self.client
            .get(admin_url)
            .send()
            .await
            .map_err(|e| anyhow!("Error in send status: {e}"))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("Failed to read response body: {e}"))?;
        if status.is_success() {
            Ok(body)
        } else {
            Err(anyhow!("API send failed: {} {}", status, body))
        }
    }
}
