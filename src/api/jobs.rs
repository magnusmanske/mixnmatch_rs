//! Job management endpoints.

use crate::api::common::{self, ApiError, Params, json_resp, ok};
use crate::app_state::AppState;
use crate::auth;
use axum::response::Response;
use std::sync::OnceLock;
use tower_sessions::Session;
use wikimisc::timestamp::TimeStamp;

fn re_action_name() -> &'static regex::Regex {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"^[a-z_]+$").expect("valid regex"))
}

pub async fn query_get_jobs(app: &AppState, params: &Params) -> Result<Response, ApiError> {
    let cid = common::get_param_int(params, "catalog", 0) as usize;
    let start = common::get_param_int(params, "start", 0) as usize;
    let max = common::get_param_int(params, "max", 50) as usize;
    let status_filter = common::get_param(params, "status_filter", "");
    let (stats, jobs, total) = app.storage().api_get_jobs(cid, start, max, &status_filter).await?;
    let mut out = serde_json::json!({"status": "OK", "data": jobs, "total": total});
    if cid == 0 {
        out["stats"] = serde_json::json!(stats);
    }
    Ok(json_resp(out))
}

pub async fn query_start_new_job(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let cid = common::get_catalog(params)?;
    let action = common::get_param(params, "action", "")
        .trim()
        .to_lowercase();
    let user = auth::guard::require_user_from_params(app, session, params).await?;
    if !re_action_name().is_match(&action) {
        return Err(ApiError::BadRequest(format!("Bad action: '{action}'")));
    }
    let valid = app.storage().api_get_existing_job_actions().await?;
    if !valid.contains(&action) {
        return Err(ApiError::BadRequest(format!("Unknown action: '{action}'")));
    }
    crate::job::Job::queue_simple_job_for_user(app, cid, &action, None, user.mnm_user_id).await?;
    Ok(ok(serde_json::json!({})))
}

pub async fn query_manage_job(
    app: &AppState,
    session: &Session,
    params: &Params,
) -> Result<Response, ApiError> {
    let user = auth::guard::require_user_from_params(app, session, params).await?;
    let job_id = common::get_param_usize(params, "job_id")?;
    let action = common::get_param(params, "action", "");

    let job = app
        .storage()
        .jobs_row_from_id(job_id)
        .await
        .map_err(|_| ApiError::NotFound(format!("No job with id {job_id}")))?;

    if !is_job_manager(app, &user, &job).await? {
        return Err(ApiError::Internal(
            "Not authorized to manage this job".into(),
        ));
    }

    let new_status = match action.as_str() {
        "stop" => crate::job_status::JobStatus::Deactivated,
        "pause" => crate::job_status::JobStatus::Paused,
        "resume" => crate::job_status::JobStatus::Todo,
        _ => return Err(ApiError::BadRequest(format!("Unknown action: '{action}'"))),
    };

    app.storage()
        .jobs_set_status(&new_status, job_id, TimeStamp::now())
        .await?;
    Ok(ok(serde_json::json!({})))
}

/// Returns true if `user` is allowed to stop/pause/resume `job`.
/// Allowed when: catalog admin, owns the job, or the job belongs to the "automatic" system user.
async fn is_job_manager(
    app: &AppState,
    user: &auth::guard::AuthedUser,
    job: &crate::job_row::JobRow,
) -> Result<bool, ApiError> {
    // Dev mode: Magnus Manske is always admin.
    if auth::guard::dev_bypass_user().is_some() {
        return Ok(true);
    }
    is_job_manager_authorized(app, &user.wikidata_username, user.mnm_user_id, job.user_id).await
}

/// Pure authorization logic — runs in production (toolforge) when
/// [`auth::guard::dev_bypass_user`] returns `None`. Extracted so unit tests
/// can hit the branches without faking the toolforge sentinel file.
///
/// Returns `Ok(true)` if any of the following holds:
///   1. `username` is flagged as a catalog admin in the `user` table.
///   2. `mnm_user_id` is non-zero and equals `job_user_id` (the requester owns the job).
///   3. The job's `user_id` matches the row for the special `"automatic"` system user.
async fn is_job_manager_authorized(
    app: &AppState,
    username: &str,
    mnm_user_id: usize,
    job_user_id: usize,
) -> Result<bool, ApiError> {
    let user_info = app
        .storage()
        .get_user_by_name(username)
        .await
        .map_err(|e| ApiError::Internal(format!("Admin check failed: {e}")))?;
    if matches!(user_info, Some((_, _, true))) {
        return Ok(true);
    }
    // mnm_user_id==0 is the "unattributed" sentinel — must NOT match a
    // job that was also written with user_id=0, or every anonymous request
    // could manage every automatic job.
    if mnm_user_id != 0 && job_user_id == mnm_user_id {
        return Ok(true);
    }
    if let Ok(Some((automatic_id, _, _))) = app.storage().get_user_by_name("automatic").await {
        if job_user_id == automatic_id {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use axum::body::to_bytes;
    use std::sync::Arc;

    fn params_from(pairs: &[(&str, &str)]) -> Params {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn empty_session() -> tower_sessions::Session {
        let store = Arc::new(tower_sessions::MemoryStore::default());
        tower_sessions::Session::new(None, store, None)
    }

    /// Drain a handler `Response` to a `serde_json::Value` so tests can
    /// assert on the JSON body shape.
    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    // ── re_action_name regex ─────────────────────────────────────────────

    #[test]
    fn re_action_name_accepts_snake_case() {
        let re = re_action_name();
        for ok in ["automatch", "auxiliary_matcher", "purge_automatches", "import"] {
            assert!(re.is_match(ok), "regex must accept {ok:?}");
        }
    }

    #[test]
    fn re_action_name_rejects_non_lowercase_or_special() {
        let re = re_action_name();
        for bad in ["Automatch", "auto-match", "match 1", "match!", "", "auto1", "auto match"] {
            assert!(!re.is_match(bad), "regex must reject {bad:?}");
        }
    }

    // ── query_get_jobs ────────────────────────────────────────────────────

    #[tokio::test]
    async fn query_get_jobs_empty_catalog_returns_zero_total() {
        let app = test_support::test_app().await;
        let cid = test_support::unique_catalog_id();
        let params = params_from(&[("catalog", &cid.to_string())]);
        let resp = query_get_jobs(&app, &params).await.unwrap();
        let body = body_json(resp).await;
        assert_eq!(body["status"], "OK");
        assert_eq!(body["total"].as_u64().unwrap(), 0);
        // Catalog-scoped response must NOT include the stats block.
        assert!(body.get("stats").is_none(), "stats key must be absent when cid != 0");
    }

    #[tokio::test]
    async fn query_get_jobs_returns_seeded_jobs_for_catalog() {
        let app = test_support::test_app().await;
        let (cid, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        test_support::seed_job("automatch", cid).await.unwrap();
        test_support::seed_job("microsync", cid).await.unwrap();
        let params = params_from(&[("catalog", &cid.to_string())]);
        let body = body_json(query_get_jobs(&app, &params).await.unwrap()).await;
        assert_eq!(body["total"].as_u64().unwrap(), 2);
        let actions: Vec<&str> = body["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["action"].as_str().unwrap())
            .collect();
        assert!(actions.contains(&"automatch") && actions.contains(&"microsync"));
    }

    #[tokio::test]
    async fn query_get_jobs_with_cid_zero_includes_stats_key() {
        let app = test_support::test_app().await;
        let params = params_from(&[("catalog", "0")]);
        let body = body_json(query_get_jobs(&app, &params).await.unwrap()).await;
        assert!(body.get("stats").is_some(), "stats key must be present when cid == 0");
    }

    // ── query_start_new_job ───────────────────────────────────────────────

    #[tokio::test]
    async fn query_start_new_job_bad_action_format_returns_bad_request() {
        let app = test_support::test_app().await;
        let cid = test_support::unique_catalog_id();
        let session = empty_session();
        // Action contains an exclamation mark — regex must reject before
        // touching the storage layer's "known actions" list.
        let params = params_from(&[("catalog", &cid.to_string()), ("action", "Auto!match")]);
        let err = query_start_new_job(&app, &session, &params).await.unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
        assert!(err.message().contains("Bad action"));
    }

    #[tokio::test]
    async fn query_start_new_job_empty_action_returns_bad_request() {
        let app = test_support::test_app().await;
        let cid = test_support::unique_catalog_id();
        let session = empty_session();
        let params = params_from(&[("catalog", &cid.to_string()), ("action", "")]);
        let err = query_start_new_job(&app, &session, &params).await.unwrap_err();
        // Empty action gets caught by either the regex (it requires +) or
        // the unknown-action check, depending on order — we just require
        // the request to be rejected as BadRequest.
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn query_start_new_job_unknown_action_returns_bad_request() {
        let app = test_support::test_app().await;
        let cid = test_support::unique_catalog_id();
        let session = empty_session();
        let params = params_from(&[
            ("catalog", &cid.to_string()),
            ("action", "definitely_not_a_real_action"),
        ]);
        let err = query_start_new_job(&app, &session, &params).await.unwrap_err();
        match &err {
            ApiError::BadRequest(m) => assert!(m.contains("Unknown action"), "got {m:?}"),
            _ => panic!("expected BadRequest, got {err:?}"),
        }
    }

    #[tokio::test]
    async fn query_start_new_job_missing_catalog_returns_bad_request() {
        let app = test_support::test_app().await;
        let session = empty_session();
        let params = params_from(&[("action", "automatch")]);
        let err = query_start_new_job(&app, &session, &params).await.unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn query_start_new_job_happy_path_inserts_row_attributed_to_dev_user() {
        let app = test_support::test_app().await;
        // `api_get_existing_job_actions` derives the "known actions" set
        // from rows already in `jobs` / `job_sizes`. In a fresh test
        // container that set is empty, so seed a row in a *different*
        // catalog with the action we want to use — the action becomes
        // "known", and our target catalog is still free of an `automatch`
        // job so it doesn't trip the UNIQUE(action, catalog) constraint.
        let other_cid = test_support::unique_catalog_id();
        test_support::seed_job("automatch", other_cid).await.unwrap();
        let (cid, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        let session = empty_session();
        let params = params_from(&[("catalog", &cid.to_string()), ("action", "automatch")]);
        let resp = query_start_new_job(&app, &session, &params).await.unwrap();
        assert_eq!(body_json(resp).await["status"], "OK");

        // Verify a job row landed in the DB with the dev-bypass user (uid 2).
        let body = body_json(
            query_get_jobs(&app, &params_from(&[("catalog", &cid.to_string())]))
                .await
                .unwrap(),
        )
        .await;
        let jobs = body["data"].as_array().unwrap();
        assert_eq!(jobs.len(), 1, "exactly one job must be queued for our catalog");
        assert_eq!(jobs[0]["action"], "automatch");
        assert_eq!(jobs[0]["user_id"].as_u64().unwrap(), 2, "must be attributed to dev-bypass uid 2");
    }

    // ── query_manage_job ──────────────────────────────────────────────────

    #[tokio::test]
    async fn query_manage_job_missing_job_id_returns_bad_request() {
        let app = test_support::test_app().await;
        let session = empty_session();
        let params = params_from(&[("action", "stop")]);
        let err = query_manage_job(&app, &session, &params).await.unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn query_manage_job_unknown_job_id_returns_not_found() {
        let app = test_support::test_app().await;
        let session = empty_session();
        // Pick an id far beyond any seeded job in this test process.
        let params = params_from(&[("job_id", "999999999"), ("action", "stop")]);
        let err = query_manage_job(&app, &session, &params).await.unwrap_err();
        assert!(matches!(err, ApiError::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn query_manage_job_stop_pause_resume_persists_each_status() {
        use crate::job_status::JobStatus;
        let app = test_support::test_app().await;
        let (cid, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        let session = empty_session();

        // UNIQUE(action, catalog) in the `jobs` table means each seeded job
        // in this catalog needs a distinct action string. Names are
        // arbitrary — `query_manage_job` does not validate them against
        // the known-actions set, only against {stop, pause, resume}.
        for (seed_action, http_action, expected) in [
            ("mgr_test_stop", "stop", JobStatus::Deactivated),
            ("mgr_test_pause", "pause", JobStatus::Paused),
            ("mgr_test_resume", "resume", JobStatus::Todo),
        ] {
            let job_id = test_support::seed_job(seed_action, cid).await.unwrap();
            let params = params_from(&[
                ("job_id", &job_id.to_string()),
                ("action", http_action),
            ]);
            query_manage_job(&app, &session, &params).await.unwrap();

            let row = app.storage().jobs_row_from_id(job_id).await.unwrap();
            assert_eq!(
                row.status, expected,
                "action={http_action} must set status to {expected:?}",
            );
        }
    }

    #[tokio::test]
    async fn query_manage_job_unknown_action_returns_bad_request() {
        let app = test_support::test_app().await;
        let (cid, _) = test_support::seed_minimal_entry(&app).await.unwrap();
        let session = empty_session();
        let job_id = test_support::seed_job("automatch", cid).await.unwrap();
        let params = params_from(&[
            ("job_id", &job_id.to_string()),
            ("action", "explode"),
        ]);
        let err = query_manage_job(&app, &session, &params).await.unwrap_err();
        match &err {
            ApiError::BadRequest(m) => assert!(m.contains("Unknown action"), "got {m:?}"),
            _ => panic!("expected BadRequest, got {err:?}"),
        }
    }

    // ── is_job_manager_authorized (auth logic, dev-bypass-free path) ─────

    #[tokio::test]
    async fn is_job_manager_authorized_admin_user_returns_true() {
        let app = test_support::test_app().await;
        let admin_name = format!("admin_{}", test_support::unique_catalog_id());
        let _admin_id = test_support::seed_user(&admin_name, true).await.unwrap();
        // Even a job owned by someone else: admin still wins.
        let allowed = is_job_manager_authorized(&app, &admin_name, /*mnm_user_id*/ 999, /*job_user_id*/ 12345)
            .await
            .unwrap();
        assert!(allowed, "catalog admin must be authorized regardless of ownership");
    }

    #[tokio::test]
    async fn is_job_manager_authorized_owner_returns_true() {
        let app = test_support::test_app().await;
        let name = format!("owner_{}", test_support::unique_catalog_id());
        let uid = test_support::seed_user(&name, false).await.unwrap();
        let allowed = is_job_manager_authorized(&app, &name, uid, uid).await.unwrap();
        assert!(allowed, "owner must be authorized to manage their own job");
    }

    #[tokio::test]
    async fn is_job_manager_authorized_unattributed_user_cannot_manage_unattributed_job() {
        // Specific concern: mnm_user_id == 0 (unauthenticated/legacy) and
        // job_user_id == 0 (legacy job with no owner) must NOT auto-match,
        // or every anonymous request could manage every legacy job.
        //
        // Seed a non-admin "automatic" user so that, if a parallel test
        // hasn't already done so, our `get_user_by_name("automatic")`
        // lookup returns a row whose id is non-zero (AUTO_INCREMENT
        // starts at 1). job_user_id=0 therefore cannot equal automatic_id,
        // and we genuinely exercise the "all branches fail" return.
        let app = test_support::test_app().await;
        let _ = test_support::seed_user("automatic", false).await;
        let name = format!("anon_{}", test_support::unique_catalog_id());
        let _ = test_support::seed_user(&name, false).await.unwrap();
        let allowed = is_job_manager_authorized(&app, &name, 0, 0).await.unwrap();
        assert!(!allowed, "uid 0 must not match user_id 0");
    }

    #[tokio::test]
    async fn is_job_manager_authorized_unknown_user_with_other_owner_returns_false() {
        let app = test_support::test_app().await;
        let name = format!("nobody_{}", test_support::unique_catalog_id());
        // No row seeded for `name` → get_user_by_name returns None → not admin.
        // mnm_user_id != job_user_id and mnm_user_id != 0 → not owner.
        // job_user_id is far above any AUTO_INCREMENT id this test process
        // can reach, so it cannot accidentally match a parallel test's
        // seeded "automatic" user.
        let allowed = is_job_manager_authorized(&app, &name, 1, 999_999_999).await.unwrap();
        assert!(!allowed);
    }

    #[tokio::test]
    async fn is_job_manager_authorized_automatic_user_owns_job_returns_true() {
        let app = test_support::test_app().await;
        let automatic_id = test_support::seed_user("automatic", false).await.unwrap();
        // Requesting user isn't admin and isn't the job owner, but the job
        // belongs to the special "automatic" system user, so anyone may
        // manage it.
        let randomname = format!("random_{}", test_support::unique_catalog_id());
        let _ = test_support::seed_user(&randomname, false).await.unwrap();
        let allowed = is_job_manager_authorized(&app, &randomname, 999, automatic_id)
            .await
            .unwrap();
        assert!(allowed, "automatic-owned jobs must be managable by anyone");
    }
}
