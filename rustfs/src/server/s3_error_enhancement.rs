// Copyright 2024 Albwebsolutions
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! S3 error response enhancement layer.
//!
//! Transforms minimal S3 XML error responses into professional, enterprise-grade
//! error pages with albwebfs branding, RequestId, HostId, Resource, and proper
//! structure. AWS S3-compatible format with Retry-After for throttling/503.

use bytes::Bytes;
use http::{HeaderValue, Request, Response, StatusCode, header::CONTENT_TYPE};
use http_body::Body;
use http_body_util::{BodyExt, Full};
use quick_xml::de::from_str;
use quick_xml::se::to_string;
use rustfs_config::ENV_RUSTFS_HOST_ID;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use uuid::Uuid;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Branding constants for albwebfs error responses.
pub const SERVER_HEADER_VALUE: &str = "albwebfs";
pub const HOST_ID_PREFIX: &str = "albwebfs-";

/// S3 Error XML structure (AWS-compatible format).
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename = "Error", rename_all = "PascalCase", default)]
struct S3ErrorXml {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "RequestId", skip_serializing_if = "String::is_empty")]
    request_id: String,
    #[serde(rename = "HostId", skip_serializing_if = "String::is_empty")]
    host_id: String,
    #[serde(rename = "Resource", skip_serializing_if = "String::is_empty")]
    resource: String,
    #[serde(rename = "BucketName", skip_serializing_if = "String::is_empty")]
    bucket_name: String,
    #[serde(rename = "Key", skip_serializing_if = "String::is_empty")]
    key: String,
}

/// Tower layer that enhances S3 error responses with professional branding.
pub struct S3ErrorEnhancementLayer<ReqBody, ResBody> {
    _marker: PhantomData<(ReqBody, ResBody)>,
}

impl<ReqBody, ResBody> S3ErrorEnhancementLayer<ReqBody, ResBody> {
    pub fn new() -> Self {
        Self { _marker: PhantomData }
    }
}

impl<ReqBody, ResBody> Default for S3ErrorEnhancementLayer<ReqBody, ResBody> {
    fn default() -> Self {
        Self::new()
    }
}

impl<ReqBody, ResBody> Clone for S3ErrorEnhancementLayer<ReqBody, ResBody> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

/// Wraps a service with S3 error enhancement.
///
/// This is the preferred entry point when composing the HTTP stack, as it
/// provides straightforward type inference compared to using the layer directly.
pub fn layer<S, ReqBody, ResBody>(service: S) -> S3ErrorEnhancement<S, ReqBody, ResBody>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: Body<Data = Bytes> + Send + Unpin + 'static,
    ResBody::Error: Into<BoxError> + Send,
{
    S3ErrorEnhancementLayer::<ReqBody, ResBody>::new().layer(service)
}

impl<S, ReqBody, ResBody> Layer<S> for S3ErrorEnhancementLayer<ReqBody, ResBody>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: Body<Data = Bytes> + Send + Unpin + 'static,
    ResBody::Error: Into<BoxError> + Send,
{
    type Service = S3ErrorEnhancement<S, ReqBody, ResBody>;

    fn layer(&self, inner: S) -> Self::Service {
        S3ErrorEnhancement {
            inner,
            _marker: PhantomData,
        }
    }
}

/// Service that wraps another service and enhances S3 error responses.
pub struct S3ErrorEnhancement<S, ReqBody, ResBody> {
    inner: S,
    _marker: PhantomData<(ReqBody, ResBody)>,
}

impl<S, ReqBody, ResBody> Clone for S3ErrorEnhancement<S, ReqBody, ResBody>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for S3ErrorEnhancement<S, ReqBody, ResBody>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: Body<Data = Bytes> + Send + Unpin + 'static,
    ResBody::Error: Into<BoxError> + Send,
{
    type Response = Response<Full<Bytes>>;
    type Error = S::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let uri = req.uri().clone();
        let future = inner.call(req);

        Box::pin(async move {
            let response = future.await?;

            let (mut parts, body) = response.into_parts();
            let status = parts.status;

            // Only enhance 4xx and 5xx error responses
            if !status.is_client_error() && !status.is_server_error() {
                let body = match BodyExt::collect(body).await {
                    Ok(collected) => collected.to_bytes(),
                    Err(_) => Bytes::new(),
                };
                return Ok(Response::from_parts(parts, Full::new(body)));
            }

            // Check content-type for XML
            let is_xml = parts
                .headers
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.contains("application/xml") || v.contains("text/xml"))
                .unwrap_or(false);

            let body_bytes = match BodyExt::collect(body).await {
                Ok(collected) => collected.to_bytes(),
                Err(_) => Bytes::new(),
            };

            let enhanced_body = if is_xml {
                enhance_s3_error_xml(&body_bytes, &parts.headers, status, &uri)
            } else {
                body_bytes
            };

            // Enterprise: Server header, Content-Type, X-Amz-Request-Id, Retry-After
            parts
                .headers
                .insert(http::header::SERVER, HeaderValue::from_static(SERVER_HEADER_VALUE));

            if is_xml {
                parts.headers.insert(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/xml; charset=utf-8"),
                );
            }

            if let Some(rid) = parts.headers.get("x-request-id") {
                parts.headers.insert(
                    http::header::HeaderName::from_static("x-amz-request-id"),
                    rid.clone(),
                );
            }

            if status == StatusCode::TOO_MANY_REQUESTS {
                parts.headers.insert(
                    http::header::RETRY_AFTER,
                    HeaderValue::from_static("1"),
                );
            } else if status == StatusCode::SERVICE_UNAVAILABLE {
                parts.headers.insert(
                    http::header::RETRY_AFTER,
                    HeaderValue::from_static("60"),
                );
            }

            Ok(Response::from_parts(parts, Full::new(enhanced_body)))
        })
    }
}

/// Parses bucket and key from path-style S3 URI path (e.g. /bucket, /bucket/key, /bucket/key/sub).
fn parse_bucket_key_from_path(path: &str) -> (Option<String>, Option<String>) {
    let path = path.trim_start_matches('/');
    if path.is_empty() {
        return (None, None);
    }
    let segments: Vec<&str> = path.splitn(2, '/').collect();
    let bucket = segments.first().filter(|s| !s.is_empty()).map(|s| (*s).to_string());
    let key = segments.get(1).filter(|s| !s.is_empty()).map(|s| (*s).to_string());
    (bucket, key)
}

/// Enhances S3 error XML with RequestId, HostId, Resource, BucketName, Key, and professional structure.
fn enhance_s3_error_xml(
    body: &[u8],
    headers: &http::HeaderMap,
    status: StatusCode,
    uri: &http::Uri,
) -> Bytes {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => return Bytes::copy_from_slice(body),
    };

    let (bucket, key) = parse_bucket_key_from_path(uri.path());
    let resource = uri.path().trim_start_matches('/');
    let resource = if resource.is_empty() {
        String::new()
    } else {
        format!("/{resource}")
    };

    let mut err: S3ErrorXml = match from_str(body_str) {
        Ok(e) => e,
        Err(_) => {
            let (code, message) = status_to_code_and_message(status);
            let fallback = S3ErrorXml {
                code,
                message,
                request_id: generate_request_id(headers),
                host_id: generate_host_id(),
                resource: resource.clone(),
                bucket_name: bucket.clone().unwrap_or_default(),
                key: key.clone().unwrap_or_default(),
                ..Default::default()
            };
            return build_enhanced_xml(fallback);
        }
    };

    if err.request_id.is_empty() {
        err.request_id = generate_request_id(headers);
    }
    if err.host_id.is_empty() {
        err.host_id = generate_host_id();
    }
    if err.message.is_empty() {
        let (code, message) = status_to_code_and_message(status);
        err.code = code;
        err.message = message;
    }
    if err.resource.is_empty() && !resource.is_empty() {
        err.resource = resource;
    }
    if err.bucket_name.is_empty() {
        err.bucket_name = bucket.unwrap_or_default();
    }
    if err.key.is_empty() {
        err.key = key.unwrap_or_default();
    }

    build_enhanced_xml(err)
}

fn generate_request_id(headers: &http::HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

fn generate_host_id() -> String {
    std::env::var(ENV_RUSTFS_HOST_ID)
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| format!("{}{}", HOST_ID_PREFIX, Uuid::new_v4().simple()))
}

fn status_to_code_and_message(status: StatusCode) -> (String, String) {
    match status.as_u16() {
        400 => (
            "InvalidRequest".into(),
            "The request was invalid. Please verify your request and try again.".into(),
        ),
        401 => ("Unauthorized".into(), "Authentication required. Please provide valid credentials.".into()),
        403 => (
            "AccessDenied".into(),
            "Access denied. You do not have permission to perform this operation.".into(),
        ),
        404 => ("NoSuchKey".into(), "The specified resource does not exist.".into()),
        405 => (
            "MethodNotAllowed".into(),
            "The specified method is not allowed against this resource.".into(),
        ),
        409 => (
            "Conflict".into(),
            "The request could not be completed due to a conflict with the current state.".into(),
        ),
        416 => ("InvalidRange".into(), "The requested range is not satisfiable.".into()),
        429 => (
            "SlowDown".into(),
            "Too many requests. Please reduce your request rate and try again.".into(),
        ),
        500 => ("InternalError".into(), "We encountered an internal error. Please try again later.".into()),
        503 => (
            "ServiceUnavailable".into(),
            "The service is temporarily unavailable. Please retry your request.".into(),
        ),
        _ => ("InternalError".into(), format!("An error occurred ({}). Please try again.", status)),
    }
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_key_from_path() {
        assert_eq!(parse_bucket_key_from_path(""), (None, None));
        assert_eq!(parse_bucket_key_from_path("/"), (None, None));
        assert_eq!(parse_bucket_key_from_path("/bucket"), (Some("bucket".into()), None));
        assert_eq!(parse_bucket_key_from_path("/bucket/"), (Some("bucket".into()), None));
        assert_eq!(parse_bucket_key_from_path("/bucket/key"), (Some("bucket".into()), Some("key".into())));
        assert_eq!(
            parse_bucket_key_from_path("/bucket/key/sub"),
            (Some("bucket".into()), Some("key/sub".into()))
        );
    }
}

fn build_enhanced_xml(err: S3ErrorXml) -> Bytes {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>{}"#,
        to_string(&err).unwrap_or_else(|_| {
            let mut extra = String::new();
            if !err.resource.is_empty() {
                extra.push_str(&format!(r#"<Resource>{}</Resource>"#, escape_xml(&err.resource)));
            }
            if !err.bucket_name.is_empty() {
                extra.push_str(&format!(r#"<BucketName>{}</BucketName>"#, escape_xml(&err.bucket_name)));
            }
            if !err.key.is_empty() {
                extra.push_str(&format!(r#"<Key>{}</Key>"#, escape_xml(&err.key)));
            }
            format!(
                r#"<Error><Code>{}</Code><Message>{}</Message><RequestId>{}</RequestId><HostId>{}</HostId>{}</Error>"#,
                escape_xml(&err.code),
                escape_xml(&err.message),
                err.request_id,
                err.host_id,
                extra
            )
        })
    );
    Bytes::from(xml)
}
