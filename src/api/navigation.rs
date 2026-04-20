//! Small navigation handlers: external redirects, entry-URL proxy, CERSEI shortcut.

use crate::api::common::{self, ApiError, Params};
use crate::app_state::AppState;
use axum::response::{IntoResponse, Response};
use std::sync::OnceLock;

/// Reused HTTP client for the entry-URL proxy. Built once so we keep
/// connection pooling across requests instead of re-doing TLS handshakes.
fn proxy_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

pub async fn query_redirect(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    let ext_id = common::get_param(params, "ext_id", "");
    let entry = crate::entry::Entry::from_ext_id(catalog, &ext_id, app).await?;
    let html = format!(
        "<html><head><META http-equiv=\"refresh\" content=\"0;URL={}\"></head><body></body></html>",
        entry.ext_url
    );
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")],
        html,
    )
        .into_response())
}

pub async fn query_proxy_entry_url(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let eid = common::get_param_int(params, "entry_id", 0) as usize;
    let entry = crate::entry::Entry::from_id(eid, app).await?;
    let body = proxy_client()
        .get(&entry.ext_url)
        .send()
        .await
        .map_err(|e| ApiError(e.to_string()))?
        .text()
        .await
        .map_err(|e| ApiError(e.to_string()))?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=UTF-8")],
        body,
    )
        .into_response())
}

pub async fn query_cersei_forward(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let sid = common::get_param_int(params, "scraper", 0) as usize;
    match app.storage().api_get_cersei_catalog(sid).await? {
        Some(cid) => {
            let url = format!("/#/catalog/{cid}");
            Ok((
                axum::http::StatusCode::FOUND,
                [(axum::http::header::LOCATION, url.as_str())],
            )
                .into_response())
        }
        None => Err(ApiError(format!(
            "No catalog associated with CERSEI scraper {sid}"
        ))),
    }
}
