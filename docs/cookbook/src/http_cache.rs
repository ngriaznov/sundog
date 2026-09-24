//! An axum middleware that caches successful `GET` responses in a
//! `Replicated` cache: one node renders a response and every node serves it
//! for the entry's lifetime.

use std::time::Duration;

use axum::body::{Body, HttpBody as _, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sundog::{Cache, CacheError, Cluster, Mode};

// ANCHOR: types
/// A response worth replaying: status, content type and the whole body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedResponse {
    status: u16,
    content_type: Option<String>,
    body: Vec<u8>,
}

impl IntoResponse for CachedResponse {
    fn into_response(self) -> Response {
        let mut response = Response::new(Body::from(self.body));
        *response.status_mut() = StatusCode::from_u16(self.status).unwrap_or(StatusCode::OK);
        if let Some(value) = self
            .content_type
            .and_then(|ct| HeaderValue::from_str(&ct).ok())
        {
            response.headers_mut().insert(header::CONTENT_TYPE, value);
        }
        response
    }
}
// ANCHOR_END: types

/// Opens the response cache.
///
/// # Errors
///
/// Returns an error if the cache cannot open.
// ANCHOR: open
pub async fn open_response_cache(
    cluster: &Cluster,
) -> Result<Cache<String, CachedResponse>, CacheError> {
    cluster
        .cache::<String, CachedResponse>("http-responses")
        .mode(Mode::Replicated)
        .ttl(Duration::from_secs(30))
        .open()
        .await
}
// ANCHOR_END: open

// ANCHOR: middleware
/// Responses larger than this pass through uncached.
const MAX_CACHED_BODY: usize = 256 * 1024;

pub async fn cache_get_responses(
    State(cache): State<Cache<String, CachedResponse>>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::GET {
        return next.run(request).await;
    }
    let key = request.uri().to_string();
    if let Some(hit) = cache.get(&key).await {
        return hit.into_response();
    }

    let response = next.run(request).await;
    // Only a body whose exact length is known and small gets buffered; a
    // stream passes through untouched.
    let small = response
        .body()
        .size_hint()
        .exact()
        .is_some_and(|len| len <= MAX_CACHED_BODY as u64);
    if response.status() != StatusCode::OK || !small {
        return response;
    }

    let (parts, body) = response.into_parts();
    let Ok(bytes) = to_bytes(body, MAX_CACHED_BODY).await else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let cached = CachedResponse {
        status: parts.status.as_u16(),
        content_type: parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string),
        body: bytes.to_vec(),
    };
    // A failed fill costs one extra render on the next request, nothing more.
    let _ = cache.insert(key, cached).await;
    Response::from_parts(parts, Body::from(bytes))
}
// ANCHOR_END: middleware

// ANCHOR: purge
/// Drops a cached page on every node, for the handler that changed it.
///
/// # Errors
///
/// Returns an error if the key fails to encode.
pub async fn purge(cache: &Cache<String, CachedResponse>, path: &str) -> Result<(), CacheError> {
    cache.remove(&path.to_string()).await
}
// ANCHOR_END: purge

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::http::Request as HttpRequest;
    use axum::middleware::from_fn_with_state;
    use axum::routing::get;
    use http_body_util::BodyExt as _;
    use tower::ServiceExt as _;

    use super::*;
    use crate::test_support::solo_cluster;

    // ANCHOR: router
    fn app(cache: Cache<String, CachedResponse>, renders: Arc<AtomicUsize>) -> Router {
        Router::new()
            .route(
                "/report",
                get(move || {
                    let renders = Arc::clone(&renders);
                    async move {
                        renders.fetch_add(1, Ordering::SeqCst);
                        "rendered once"
                    }
                }),
            )
            .layer(from_fn_with_state(cache, cache_get_responses))
    }
    // ANCHOR_END: router

    async fn body_of(router: &Router) -> String {
        let response = router
            .clone()
            .oneshot(
                HttpRequest::get("/report")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("route answers");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        String::from_utf8(bytes.to_vec()).expect("utf-8 body")
    }

    #[tokio::test]
    async fn a_second_get_is_served_from_the_cache_and_a_purge_renders_again() {
        let cluster = solo_cluster("cookbook-http-cache").await;
        let cache = open_response_cache(&cluster).await.expect("cache opens");
        let renders = Arc::new(AtomicUsize::new(0));
        let router = app(cache.clone(), Arc::clone(&renders));

        assert_eq!(body_of(&router).await, "rendered once");
        assert_eq!(body_of(&router).await, "rendered once");
        assert_eq!(renders.load(Ordering::SeqCst), 1, "the second GET is a hit");

        purge(&cache, "/report").await.expect("purge");
        assert_eq!(body_of(&router).await, "rendered once");
        assert_eq!(renders.load(Ordering::SeqCst), 2, "a purge forces a render");
        cluster.shutdown().await;
    }
}
