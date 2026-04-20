use anyhow::{Result, anyhow};
use ini::Ini;
use serde_json::Value;

/// Base URL for the MediaWiki OAuth endpoints — the *central* OAuth wiki.
/// Mirrors PHP `MW_OAuth::$mwOAuthUrl`. All three handshake steps (initiate,
/// token, authorize) go here regardless of which wiki the user is editing.
pub const MW_OAUTH_BASE_URL: &str = "https://www.mediawiki.org/wiki/Special:OAuth";

/// The wiki on which we fetch `meta=userinfo` after login. Mirrors PHP
/// `MW_OAuth::$apiUrl` when the class is constructed with `('wikidata','wikidata')`.
pub const MW_API_URL: &str = "https://www.wikidata.org/w/api.php";

/// Default path to the consumer credentials file on toolforge.
pub const DEFAULT_INI_PATH: &str = "/data/project/mix-n-match/oauth.ini";

#[derive(Clone)]
pub struct OauthConfig {
    pub agent: String,
    pub consumer_key: String,
    pub consumer_secret: String,
    pub callback_url: String,
    pub cookie_name: String,
    pub cookie_secure: bool,
    pub session_lifetime_days: i64,
    /// Directory holding one JSON file per active session.
    /// Sessions are kept across restarts so users stay logged in.
    pub session_dir: String,
}

impl std::fmt::Debug for OauthConfig {
    // Never leak the consumer secret.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OauthConfig")
            .field("agent", &self.agent)
            .field("consumer_key", &self.consumer_key)
            .field("consumer_secret", &"<redacted>")
            .field("callback_url", &self.callback_url)
            .field("cookie_name", &self.cookie_name)
            .field("cookie_secure", &self.cookie_secure)
            .field("session_lifetime_days", &self.session_lifetime_days)
            .field("session_dir", &self.session_dir)
            .finish()
    }
}

impl OauthConfig {
    /// Build the OAuth configuration from the top-level `config.json` value.
    ///
    /// Expects an `oauth` object with keys:
    /// `ini_path`, `callback_url`, `session_db_path`, `cookie_name`,
    /// `cookie_secure`, `session_lifetime_days`. Any omitted key falls back to
    /// a safe default. The `consumer_key` / `consumer_secret` / `agent` fields
    /// are read from the ini file pointed to by `ini_path`.
    pub fn from_app_config(cfg: &Value) -> Result<Self> {
        let oauth = &cfg["oauth"];
        let ini_path = oauth["ini_path"]
            .as_str()
            .unwrap_or(DEFAULT_INI_PATH)
            .to_string();
        let callback_url = oauth["callback_url"]
            .as_str()
            .ok_or_else(|| anyhow!("config.oauth.callback_url not set"))?
            .to_string();
        let cookie_name = oauth["cookie_name"]
            .as_str()
            .unwrap_or("mnm_session")
            .to_string();
        let cookie_secure = oauth["cookie_secure"].as_bool().unwrap_or(true);
        let session_lifetime_days =
            oauth["session_lifetime_days"].as_i64().unwrap_or(90);
        let session_dir = oauth["session_dir"]
            .as_str()
            .unwrap_or("./sessions")
            .to_string();

        let (agent, consumer_key, consumer_secret) = load_ini(&ini_path)?;
        Ok(Self {
            agent,
            consumer_key,
            consumer_secret,
            callback_url,
            cookie_name,
            cookie_secure,
            session_lifetime_days,
            session_dir,
        })
    }
}

/// Parse a mix-n-match-style `oauth.ini`. Fails fast if any required key is
/// missing or empty — a misconfigured OAuth setup would otherwise surface as a
/// runtime login error on first use.
fn load_ini(path: &str) -> Result<(String, String, String)> {
    let ini = Ini::load_from_file(path)
        .map_err(|e| anyhow!("Cannot read oauth.ini at '{path}': {e}"))?;
    // The PHP code uses `parse_ini_file` without a section; rust-ini exposes
    // such entries in the `None` section, but mix-n-match's real file wraps
    // them in `[settings]`. Accept both.
    let section = ini
        .section(Some("settings"))
        .or_else(|| ini.section::<String>(None))
        .ok_or_else(|| anyhow!("oauth.ini '{path}' has no usable section"))?;

    let agent = get_nonempty(section, "agent", path)?;
    let consumer_key = get_nonempty(section, "consumerKey", path)?;
    let consumer_secret = get_nonempty(section, "consumerSecret", path)?;
    Ok((agent, consumer_key, consumer_secret))
}

fn get_nonempty(section: &ini::Properties, key: &str, path: &str) -> Result<String> {
    let value = section
        .get(key)
        .ok_or_else(|| anyhow!("oauth.ini '{path}' missing key '{key}'"))?
        .trim();
    if value.is_empty() {
        return Err(anyhow!("oauth.ini '{path}' has empty '{key}'"));
    }
    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(contents: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("oauth_test_{}.ini", uuid::Uuid::new_v4()));
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        path
    }

    #[test]
    fn parses_ini_with_settings_section() {
        let p = write_tmp(
            "[settings]\nagent = mix-n-match\nconsumerKey = abc\nconsumerSecret = def\n",
        );
        let (a, k, s) = load_ini(p.to_str().unwrap()).unwrap();
        std::fs::remove_file(&p).ok();
        assert_eq!(a, "mix-n-match");
        assert_eq!(k, "abc");
        assert_eq!(s, "def");
    }

    #[test]
    fn rejects_missing_key() {
        let p = write_tmp("[settings]\nagent = x\nconsumerKey = y\n");
        let err = load_ini(p.to_str().unwrap()).unwrap_err().to_string();
        std::fs::remove_file(&p).ok();
        assert!(err.contains("consumerSecret"), "err was: {err}");
    }

    #[test]
    fn rejects_empty_value() {
        let p = write_tmp(
            "[settings]\nagent = mix-n-match\nconsumerKey =\nconsumerSecret = x\n",
        );
        let err = load_ini(p.to_str().unwrap()).unwrap_err().to_string();
        std::fs::remove_file(&p).ok();
        assert!(err.contains("consumerKey"), "err was: {err}");
    }

    #[test]
    fn rejects_missing_file() {
        let err = load_ini("/nonexistent/path/oauth.ini")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Cannot read oauth.ini"));
    }
}
