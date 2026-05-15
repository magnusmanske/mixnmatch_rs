//! A small persistent `tower_sessions::SessionStore` that writes each session
//! as a JSON file on disk. Written because the published
//! `tower-sessions-sqlx-store` and `tower-sessions-file-based-store` crates
//! still track `tower-sessions-core 0.14`, incompatible with the 0.15 we use.
//!
//! One file per session (named after the session id) keeps the implementation
//! trivial and means restarting the server preserves logins. Expired
//! sessions are dropped on read; we never scan the directory. That's fine
//! for the volume of logins we expect.

use async_trait::async_trait;
use std::io::ErrorKind;
use std::path::PathBuf;
use tower_sessions::session::{Id, Record};
use tower_sessions::session_store::{Error, Result};
use tower_sessions::SessionStore;

#[derive(Debug, Clone)]
pub struct FileSessionStore {
    dir: PathBuf,
}

impl FileSessionStore {
    /// Create a store writing to `dir`, creating the directory if missing.
    ///
    /// On Unix, the directory is forced to `0o700` (owner-only access) and
    /// every session file is written with `0o600`. Session records contain
    /// OAuth access tokens — the secrets the bot uses to edit Wikidata as
    /// the user — so the per-user umask default of 0o022 (world-readable)
    /// is unacceptable. Audit reference: M-5 in
    /// `audits/comprehensive_security_report.md`.
    pub fn new(dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Idempotent: tighten perms whether we created the dir or it
            // already existed. The umask of the creating process is
            // irrelevant after this.
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(Self { dir })
    }

    fn path_for(&self, id: &Id) -> PathBuf {
        // Id's Display is a 22-char url-safe base64 string — safe as a filename.
        self.dir.join(format!("{id}.json"))
    }
}

/// On Unix, set file mode to `0o600` (owner-only read/write). No-op on
/// other platforms — Windows has no equivalent and the deployment target
/// is Linux (Toolforge). Errors are swallowed because the file already
/// holds valid content; failing here would force the user to re-login
/// every time chmod fails.
#[cfg(unix)]
async fn set_session_file_mode_0600(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await;
}
#[cfg(not(unix))]
async fn set_session_file_mode_0600(_path: &std::path::Path) {}

#[async_trait]
impl SessionStore for FileSessionStore {
    async fn create(&self, record: &mut Record) -> Result<()> {
        // Guard against ID collisions by rolling a new id until we find one
        // that doesn't already exist on disk. Sessions are i128 so collisions
        // are astronomically unlikely, but the trait contract asks us to handle it.
        loop {
            let path = self.path_for(&record.id);
            match tokio::fs::metadata(&path).await {
                Err(e) if e.kind() == ErrorKind::NotFound => break,
                Err(e) => return Err(Error::Backend(e.to_string())),
                Ok(_) => record.id = Id::default(),
            }
        }
        self.save(record).await
    }

    async fn save(&self, record: &Record) -> Result<()> {
        let json = serde_json::to_vec(record).map_err(|e| Error::Encode(e.to_string()))?;
        let path = self.path_for(&record.id);
        // Write to a sibling temp file then rename — atomic on the same
        // filesystem, so a concurrent `load` never sees a half-written file.
        let tmp = path.with_extension("json.tmp");
        tokio::fs::write(&tmp, &json)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        // chmod *before* rename: on Unix, mode is per-inode and survives
        // the rename, so racing a `load` against a creating writer can't
        // see a tmp-mode file. Belt-and-braces alongside the dir 0o700.
        set_session_file_mode_0600(&tmp).await;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> Result<Option<Record>> {
        let path = self.path_for(session_id);
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::Backend(e.to_string())),
        };
        let record: Record =
            serde_json::from_slice(&bytes).map_err(|e| Error::Decode(e.to_string()))?;
        // Drop expired sessions on read. tower-sessions also checks expiry,
        // but cleaning up here keeps old files from accumulating forever.
        if record.expiry_date < time::OffsetDateTime::now_utc() {
            // Log at debug — frequent enough during normal operation that
            // info would be noise, but useful when triaging "why am I being
            // logged out?" reports.
            log::debug!("removing expired session {session_id}");
            if let Err(e) = tokio::fs::remove_file(&path).await {
                log::warn!("failed to remove expired session file {session_id}: {e}");
            }
            return Ok(None);
        }
        Ok(Some(record))
    }

    async fn delete(&self, session_id: &Id) -> Result<()> {
        match tokio::fs::remove_file(self.path_for(session_id)).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Backend(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower_sessions::cookie::time::Duration;

    fn tmp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("mnm_file_store_{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn save_load_roundtrip() {
        let dir = tmp_dir();
        let store = FileSessionStore::new(dir.clone()).unwrap();
        let mut rec = Record {
            id: Id::default(),
            data: Default::default(),
            expiry_date: time::OffsetDateTime::now_utc() + Duration::hours(1),
        };
        store.create(&mut rec).await.unwrap();
        let loaded = store.load(&rec.id).await.unwrap().expect("should exist");
        assert_eq!(loaded.id, rec.id);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn expired_session_returns_none() {
        let dir = tmp_dir();
        let store = FileSessionStore::new(dir.clone()).unwrap();
        let mut rec = Record {
            id: Id::default(),
            data: Default::default(),
            expiry_date: time::OffsetDateTime::now_utc() - Duration::minutes(5),
        };
        store.create(&mut rec).await.unwrap();
        assert!(store.load(&rec.id).await.unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn delete_removes_file() {
        let dir = tmp_dir();
        let store = FileSessionStore::new(dir.clone()).unwrap();
        let mut rec = Record {
            id: Id::default(),
            data: Default::default(),
            expiry_date: time::OffsetDateTime::now_utc() + Duration::hours(1),
        };
        store.create(&mut rec).await.unwrap();
        store.delete(&rec.id).await.unwrap();
        assert!(store.load(&rec.id).await.unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn missing_session_returns_none() {
        let dir = tmp_dir();
        let store = FileSessionStore::new(dir.clone()).unwrap();
        let id = Id::default();
        assert!(store.load(&id).await.unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Session files contain OAuth access tokens. The dir must be 0o700
    /// (owner-only traversal) and every session file 0o600 (owner-only
    /// read/write). Audit reference: M-5 in
    /// `audits/comprehensive_security_report.md`.
    #[cfg(unix)]
    #[tokio::test]
    async fn dir_and_file_modes_are_tight() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp_dir();
        let store = FileSessionStore::new(dir.clone()).unwrap();

        // Directory mode — bottom 12 bits hold the perm bits we care about.
        let dir_mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            dir_mode, 0o700,
            "session dir must be 0o700, got {dir_mode:o}"
        );

        // Create a record and check the resulting file mode.
        let mut rec = Record {
            id: Id::default(),
            data: Default::default(),
            expiry_date: time::OffsetDateTime::now_utc() + Duration::hours(1),
        };
        store.create(&mut rec).await.unwrap();
        let file_path = store.path_for(&rec.id);
        let file_mode =
            std::fs::metadata(&file_path).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            file_mode, 0o600,
            "session file must be 0o600, got {file_mode:o}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Tighten perms even on a pre-existing dir (e.g. one created earlier
    /// with a looser umask before this code was deployed).
    #[cfg(unix)]
    #[tokio::test]
    async fn dir_perms_are_tightened_on_existing_dir() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tmp_dir();
        // Create the dir up-front with a loose mode (simulates a deploy
        // upgrade from the pre-fix version).
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let _ = FileSessionStore::new(dir.clone()).unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o7777;
        assert_eq!(
            mode, 0o700,
            "FileSessionStore::new must tighten existing-dir mode to 0o700, got {mode:o}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
