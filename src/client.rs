use anyhow::Context;
use reqwest::{Method, RequestBuilder};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
    },
};

#[derive(Debug, Error)]
pub enum ControllerClientError {
    #[error("request to controller {endpoint} failed: {source}")]
    Transport {
        endpoint: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("controller request failed ({status}): {message}")]
    Http {
        endpoint: String,
        status: reqwest::StatusCode,
        body: String,
        message: String,
    },
    #[error("controller {endpoint} returned an invalid response: {source}")]
    InvalidResponse {
        endpoint: String,
        #[source]
        source: reqwest::Error,
    },
}

impl ControllerClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport { .. } => true,
            Self::Http { status, .. } => status.is_server_error(),
            Self::InvalidResponse { source, .. } => {
                source.is_body() || source.is_connect() || source.is_timeout()
            }
        }
    }
}

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

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, ControllerClientError> {
        self.send_json::<T, ()>(Method::GET, path, None).await
    }

    pub async fn send_json<T: DeserializeOwned, B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, ControllerClientError> {
        let mut request = self.request(method, path);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = self.checked(request, path).await?;
        response
            .json()
            .await
            .map_err(|source| ControllerClientError::InvalidResponse {
                endpoint: self.endpoint(path),
                source,
            })
    }

    pub async fn send_text(
        &self,
        method: Method,
        path: &str,
        content_type: Option<&str>,
        body: Option<String>,
    ) -> Result<String, ControllerClientError> {
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
            .map_err(|source| ControllerClientError::InvalidResponse {
                endpoint: self.endpoint(path),
                source,
            })
    }

    pub async fn connect_data_websocket(
        &self,
        path: &str,
        token: &str,
    ) -> anyhow::Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        let mut endpoint = url::Url::parse(&self.endpoint(path))
            .with_context(|| format!("invalid controller data endpoint {path:?}"))?;
        let websocket_scheme = match endpoint.scheme() {
            "http" => "ws",
            "https" => "wss",
            scheme => anyhow::bail!("controller URL scheme {scheme:?} does not support WebSocket"),
        };
        endpoint
            .set_scheme(websocket_scheme)
            .map_err(|_| anyhow::anyhow!("failed to construct controller WebSocket URL"))?;
        let mut request = endpoint
            .as_str()
            .into_client_request()
            .context("failed to build controller WebSocket request")?;
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid data session token")?,
        );
        let (socket, _) = connect_async(request)
            .await
            .with_context(|| format!("failed to connect to data endpoint {endpoint}"))?;
        Ok(socket)
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.http
            .request(method, self.endpoint(path))
            .bearer_auth(&self.token)
    }

    async fn checked(
        &self,
        request: RequestBuilder,
        path: &str,
    ) -> Result<reqwest::Response, ControllerClientError> {
        let endpoint = self.endpoint(path);
        let response = request
            .send()
            .await
            .map_err(|source| ControllerClientError::Transport {
                endpoint: endpoint.clone(),
                source,
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        let message = controller_error_message(&body);
        Err(ControllerClientError::Http {
            endpoint,
            status,
            body,
            message,
        })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.controller, path)
    }
}

fn controller_error_message(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("error")?.as_str().map(str::to_owned))
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| {
            let body = body.trim();
            if body.is_empty() {
                "Controller returned an empty error response".into()
            } else {
                body.into()
            }
        })
}

#[cfg(test)]
mod tests {
    use super::controller_error_message;

    #[test]
    fn extracts_human_readable_controller_error() {
        assert_eq!(
            controller_error_message(r#"{"error":"restart expects STACK.SERVICE"}"#),
            "restart expects STACK.SERVICE"
        );
        assert_eq!(controller_error_message("plain failure"), "plain failure");
        assert_eq!(
            controller_error_message(""),
            "Controller returned an empty error response"
        );
    }
}
