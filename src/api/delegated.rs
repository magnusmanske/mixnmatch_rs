//! Endpoints that delegate to the in-process micro-API. No HTTP round-trip is
//! involved; this file just wires the v1 `?query=…` shape onto the data_*
//! helpers exported by `crate::micro_api`.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use tower_sessions::Session;

pub async fn query_get_sync(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let _catalog = common::get_catalog(params)?;
    let data = crate::micro_api::data_get_sync(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_sparql_list(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_sparql_list(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_quick_compare(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let data = crate::micro_api::data_quick_compare(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_get_code_fragments(
    app: &AppState,
    params: &Params,
) -> Result<Response, ApiError> {
    let catalog = common::get_catalog(params)?;
    // Resolve the user_id concurrently with fetching the fragments — the two
    // are independent, and `get_or_create_user_id` adds an extra DB round trip.
    let username = common::get_param(params, "username", "");
    let (data, user_allowed) = tokio::join!(
        async {
            crate::micro_api::data_get_code_fragments(app, params)
                .await
                .map_err(ApiError)
        },
        async {
            if username.is_empty() {
                0_i64
            } else {
                let uid = app
                    .storage()
                    .get_or_create_user_id(&username.replace('_', " "))
                    .await
                    .unwrap_or(0);
                // Matches PHP: code_fragment_allowed_user_ids = [2]
                i64::from(uid == 2)
            }
        },
    );
    let mut data = data?;
    if let Some(obj) = data.as_object_mut() {
        obj.insert("user_allowed".into(), serde_json::json!(user_allowed));
        obj.entry("catalog")
            .or_insert_with(|| serde_json::json!(catalog));
    }
    Ok(ok(data))
}

pub async fn query_save_code_fragment(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    if uid != 2 {
        return Err(ApiError("Not allowed, ask Magnus".into()));
    }
    let data = crate::micro_api::data_save_code_fragment(app, params)
        .await
        .map_err(ApiError)?;
    Ok(ok(data))
}

pub async fn query_test_code_fragment(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let uid = auth::guard::require_user_from_params(app, session, params)
        .await?
        .mnm_user_id;
    if uid != 2 {
        return Err(ApiError("Not allowed, ask Magnus".into()));
    }
    let entry_id = common::get_param_int(params, "entry_id", 0) as usize;
    if entry_id == 0 {
        return Err(ApiError("No entry_id".into()));
    }
    let fragment_str = common::get_param(params, "fragment", "{}");
    let fragment: serde_json::Value =
        serde_json::from_str(&fragment_str).map_err(|_| ApiError("Bad fragment".into()))?;
    let function = fragment
        .get("function")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if function.is_empty() {
        return Err(ApiError(format!("Bad fragment function '{function}'")));
    }
    let mut run_params: crate::micro_api::Params = std::collections::HashMap::new();
    run_params.insert("function".into(), function.to_string());
    run_params.insert("entry_id".into(), entry_id.to_string());
    if let Some(html) = params.get("html") {
        run_params.insert("html".into(), html.clone());
    }
    let data = crate::micro_api::data_run_lua(app, &run_params)
        .await
        .map_err(ApiError)?;
    Ok(json_resp(serde_json::json!({
        "status": "OK",
        "data": data,
        "tested_via": "lua",
    })))
}
