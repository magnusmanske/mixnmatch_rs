//! Multipart upload of import files + the `/api/v1/import_catalog` REST endpoint.

use crate::api::common::{ApiError, ok};
use crate::api::router::SharedState;
use crate::app_state::AppState;
use crate::import_catalog::ImportMode;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use tower_sessions::Session;

/// Parse a multipart request that carries a `query=upload_import_file` field
/// plus the file under `import_file`. Streams the file to
/// `{import_file_path}/{uuid}` and records the upload in the `import_file`
/// table so later endpoints can resolve the UUID.
pub async fn handle_multipart_upload(
    app: &AppState,
    _session: &Session,
    req: axum::extract::Request,
) -> Response {
    use axum::extract::FromRequest;
    let mut multipart =
        match axum::extract::Multipart::from_request(req, &(app.clone())).await {
            Ok(m) => m,
            Err(e) => return ApiError(format!("multipart parse error: {e}")).into_response(),
        };

    let form = match collect_upload_fields(&mut multipart, app).await {
        Ok(f) => f,
        Err(resp) => return resp,
    };

    if form.query != "upload_import_file" {
        return ApiError(format!(
            "multipart POST only supported for query=upload_import_file (got '{}')",
            form.query
        ))
        .into_response();
    }
    if form.username.is_empty() {
        return ApiError("missing 'username' field".into()).into_response();
    }
    if form.data_format.is_empty() {
        return ApiError("missing 'data_format' field".into()).into_response();
    }
    let uuid = match form.uuid {
        Some(u) if form.file_bytes_written > 0 => u,
        _ => return ApiError("missing or empty 'import_file' field".into()).into_response(),
    };

    let user_id = match app
        .storage()
        .get_user_by_name(&form.username.replace('_', " "))
        .await
    {
        Ok(Some((id, _, _))) => id,
        Ok(None) => return ApiError(format!("unknown user '{}'", form.username)).into_response(),
        Err(e) => return ApiError(e.to_string()).into_response(),
    };

    if let Err(e) = app
        .storage()
        .save_import_file(&uuid, &form.data_format, user_id)
        .await
    {
        // Roll back the file on DB failure so we don't orphan on-disk bytes.
        let _ = tokio::fs::remove_file(format!("{}/{}", app.import_file_path(), &uuid)).await;
        return ApiError(format!("cannot record upload: {e}")).into_response();
    }

    ok(serde_json::json!({
        "uuid": uuid,
        "bytes": form.file_bytes_written,
    }))
}

struct UploadForm {
    query: String,
    data_format: String,
    username: String,
    uuid: Option<String>,
    file_bytes_written: u64,
}

/// Drain a multipart body into the fields this endpoint expects. Returns an
/// `ApiError`-shaped `Response` in `Err` so callers can early-return without
/// a second layer of match arms.
async fn collect_upload_fields(
    multipart: &mut axum::extract::Multipart,
    app: &AppState,
) -> Result<UploadForm, Response> {
    let mut form = UploadForm {
        query: String::new(),
        data_format: String::new(),
        username: String::new(),
        uuid: None,
        file_bytes_written: 0,
    };
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => return Err(ApiError(format!("multipart field error: {e}")).into_response()),
        };
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "query" => form.query = field.text().await.unwrap_or_default(),
            "data_format" => form.data_format = field.text().await.unwrap_or_default(),
            "username" => form.username = field.text().await.unwrap_or_default(),
            "import_file" => {
                let (new_uuid, bytes) = stream_import_file(field, app).await?;
                form.uuid = Some(new_uuid);
                form.file_bytes_written = bytes;
            }
            _ => {
                // Drain unknown fields so the stream advances.
                let _ = field.text().await;
            }
        }
    }
    Ok(form)
}

/// Stream a single `import_file` multipart field to disk under a fresh UUID,
/// returning `(uuid, bytes_written)`. Removes the on-disk file and returns an
/// error response on any I/O failure so we never leave half-written bytes.
async fn stream_import_file(
    mut field: axum::extract::multipart::Field<'_>,
    app: &AppState,
) -> Result<(String, u64), Response> {
    use tokio::io::AsyncWriteExt;
    // Stream the file to disk chunk by chunk — never buffer the whole thing
    // in memory; uploaded catalogs can be 100s of MB.
    let new_uuid = uuid::Uuid::new_v4().to_string();
    let path = format!("{}/{}", app.import_file_path(), &new_uuid);
    let file = tokio::fs::File::create(&path)
        .await
        .map_err(|e| ApiError(format!("cannot create upload file: {e}")).into_response())?;
    let mut writer = tokio::io::BufWriter::new(file);
    let mut bytes_written: u64 = 0;
    loop {
        match field.chunk().await {
            Ok(Some(chunk)) => {
                bytes_written += chunk.len() as u64;
                if let Err(e) = writer.write_all(&chunk).await {
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(ApiError(format!("write failed: {e}")).into_response());
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = tokio::fs::remove_file(&path).await;
                return Err(ApiError(format!("upload chunk error: {e}")).into_response());
            }
        }
    }
    if let Err(e) = writer.flush().await {
        let _ = tokio::fs::remove_file(&path).await;
        return Err(ApiError(format!("flush failed: {e}")).into_response());
    }
    Ok((new_uuid, bytes_written))
}

/// POST body for `/api/v1/import_catalog`.
///
/// Either `entries` (inline array) or `uuid` (reference to an uploaded
/// import_file) must be provided.
#[derive(serde::Deserialize)]
pub struct ImportCatalogRequest {
    catalog_id: usize,
    /// "add_replace" (default) or "add_replace_delete"
    #[serde(default = "default_import_mode")]
    mode: ImportMode,
    /// Inline MetaEntry objects. Mutually exclusive with `uuid`.
    #[serde(default)]
    entries: Option<Vec<crate::meta_entry::MetaEntry>>,
    /// UUID of a previously-uploaded import_file (type must be "json" or "jsonl").
    #[serde(default)]
    uuid: Option<String>,
}

fn default_import_mode() -> ImportMode {
    ImportMode::AddReplace
}

pub async fn api_import_catalog(
    State(app): State<SharedState>,
    axum::Json(body): axum::Json<ImportCatalogRequest>,
) -> Response {
    let result = if let Some(uuid) = &body.uuid {
        crate::import_catalog::import_from_import_file(&app, body.catalog_id, uuid, body.mode).await
    } else if let Some(entries) = body.entries {
        // Inline entries: require a user via the import_file.user equivalent.
        // For inline POST there is no import_file row, so we don't validate
        // the user field (same as CLI).
        crate::import_catalog::import_meta_entries(&app, body.catalog_id, entries, body.mode, None)
            .await
    } else {
        Err(anyhow::anyhow!(
            "Either 'entries' or 'uuid' must be provided"
        ))
    };

    match result {
        Ok(result) => {
            let data = serde_json::json!({
                "created": result.created,
                "updated": result.updated,
                "skipped_fully_matched": result.skipped_fully_matched,
                "deleted": result.deleted,
                "errors": result.errors,
            });
            ok(data)
        }
        Err(e) => ApiError(e.to_string()).into_response(),
    }
}
