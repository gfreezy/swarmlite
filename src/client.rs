use anyhow::{Context, Result, bail};
use reqwest::{Method, RequestBuilder};
use serde::{Serialize, de::DeserializeOwned};

/// Authenticated client for the single Swarmlite controller.
#[derive(Clone)]
pub struct ControllerClient {
    controller: String,
    token: String,
    http: reqwest::Client,
}

impl ControllerClient {
    pub fn new(controller: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            controller: controller.into().trim_end_matches('/').to_owned(),
            token: token.into(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.send_json::<T, ()>(Method::GET, path, None).await
    }

    pub async fn send_json<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T> {
        let mut request = self.request(method, path);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = self.checked(request, path).await?;
        response
            .json()
            .await
            .with_context(|| format!("controller {} returned invalid JSON", self.endpoint(path)))
    }

    pub async fn send_text(
        &self,
        method: Method,
        path: &str,
        content_type: Option<&str>,
        body: Option<String>,
    ) -> Result<String> {
        let mut request = self.request(method, path);
        if let Some(content_type) = content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        self.checked(request, path)
            .await?
            .text()
            .await
            .map_err(Into::into)
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.http
            .request(method, self.endpoint(path))
            .bearer_auth(&self.token)
    }

    async fn checked(&self, request: RequestBuilder, path: &str) -> Result<reqwest::Response> {
        let response = request.send().await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        bail!(
            "controller {} returned {status}: {body}",
            self.endpoint(path)
        )
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.controller, path)
    }
}
