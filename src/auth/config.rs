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
    /// Optional pre-issued access token for an owner-only OAuth1.0a
    /// consumer, used by background jobs that write back to Wikidata
    /// (e.g. the wd_matches push pipeline). Lives in `oauth.ini` under
    /// `[bot]` as `accessKey` / `accessSecret`. Both must be set for
    /// the bot to be usable; if either is absent, write-back paths
    /// must surface a clear configuration error rather than silently
    /// no-oping.
    pub bot_access_key: Option<String>,
    pub bot_access_secret: Option<String>,
}

impl std::fmt::Debug for OauthConfig {
    // Never leak the consumer secret or the bot access token.
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
            .field("bot_access_key", &self.bot_access_key)
            .field(
                "bot_access_secret",
                &self.bot_access_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl OauthConfig {
    /// True when both `[bot]` keys are present in `oauth.ini`. Callers that
    /// want to perform bot-attributed writes should check this up-front so
    /// they can fail fast with a config-error rather than partway through
    /// a batch.
    pub fn has_bot_credentials(&self) -> bool {
        self.bot_access_key.is_some() && self.bot_access_secret.is_some()
    }
}

impl OauthConfig {
    /// Build the OAuth configuration from the top-level `config.json` value.
    ///
    /// Expects an `oauth` object with keys:
    /// `ini_path`, `callback_url`, `session_db_path`, `cookie_name`,
    /// `cookie_secure`, `session_lifetime_days`. Any omitted key falls back to
    /// a safe default. The `consumer_key` / `consumer_secret` / `agent` fields
    /// are read from the ini file pointed to by `ini_path`. An optional
    /// `[bot]` section in the same ini file provides a pre-issued access
    /// token for owner-only OAuth1.0a writes.
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

        let IniContents {
            agent,
            consumer_key,
            consumer_secret,
            bot_access_key,
            bot_access_secret,
        } = load_ini(&ini_path)?;
        Ok(Self {
            agent,
            consumer_key,
            consumer_secret,
            callback_url,
            cookie_name,
            cookie_secure,
            session_lifetime_days,
            session_dir,
            bot_access_key,
            bot_access_secret,
        })
    }
}

#[derive(Debug)]
struct IniContents {
    agent: String,
    consumer_key: String,
    consumer_secret: String,
    bot_access_key: Option<String>,
    bot_access_secret: Option<String>,
}

/// Parse a mix-n-match-style `oauth.ini`. Fails fast if any required key in
/// the consumer section is missing or empty — a misconfigured consumer would
/// otherwise surface as a runtime login error on first use.
///
/// The optional `[bot]` section is parsed leniently: if both `accessKey` and
/// `accessSecret` are present and non-empty, they're returned; if either is
/// missing, both come back as `None` (the file just doesn't grant bot access)
/// rather than erroring. This way an `oauth.ini` set up purely for the
/// browser flow still loads.
fn load_ini(path: &str) -> Result<IniContents> {
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

    let (bot_access_key, bot_access_secret) = ini
        .section(Some("bot"))
        .map(|bot| (get_optional(bot, "accessKey"), get_optional(bot, "accessSecret")))
        .unwrap_or((None, None));

    Ok(IniContents {
        agent,
        consumer_key,
        consumer_secret,
        bot_access_key,
        bot_access_secret,
    })
}

/// Like `get_nonempty` but returns `None` instead of an error when the key
/// is missing or blank — used for optional `[bot]` keys that are allowed
/// to be absent.
fn get_optional(section: &ini::Properties, key: &str) -> Option<String> {
    let v = section.get(key)?.trim();
    if v.is_empty() { None } else { Some(v.to_string()) }
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
        let c = load_ini(p.to_str().unwrap()).unwrap();
        std::fs::remove_file(&p).ok();
        assert_eq!(c.agent, "mix-n-match");
        assert_eq!(c.consumer_key, "abc");
        assert_eq!(c.consumer_secret, "def");
        assert_eq!(c.bot_access_key, None);
        assert_eq!(c.bot_access_secret, None);
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

    #[test]
    fn picks_up_bot_section_when_complete() {
        let p = write_tmp(
            "[settings]\nagent = mnm\nconsumerKey = ck\nconsumerSecret = cs\n\
             [bot]\naccessKey = ak\naccessSecret = as\n",
        );
        let c = load_ini(p.to_str().unwrap()).unwrap();
        std::fs::remove_file(&p).ok();
        assert_eq!(c.bot_access_key.as_deref(), Some("ak"));
        assert_eq!(c.bot_access_secret.as_deref(), Some("as"));
    }

    #[test]
    fn skips_partial_bot_section() {
        // Only one of the two keys present → both come back None. The
        // loader is lenient because the consumer section alone is enough
        // to drive the browser flow.
        let p = write_tmp(
            "[settings]\nagent = mnm\nconsumerKey = ck\nconsumerSecret = cs\n\
             [bot]\naccessKey = ak\n",
        );
        let c = load_ini(p.to_str().unwrap()).unwrap();
        std::fs::remove_file(&p).ok();
        assert_eq!(c.bot_access_key.as_deref(), Some("ak"));
        assert_eq!(c.bot_access_secret, None);
    }

    #[test]
    fn ignores_blank_bot_keys() {
        let p = write_tmp(
            "[settings]\nagent = mnm\nconsumerKey = ck\nconsumerSecret = cs\n\
             [bot]\naccessKey =\naccessSecret =   \n",
        );
        let c = load_ini(p.to_str().unwrap()).unwrap();
        std::fs::remove_file(&p).ok();
        assert_eq!(c.bot_access_key, None);
        assert_eq!(c.bot_access_secret, None);
    }

    #[test]
    fn has_bot_credentials_requires_both_halves() {
        let cfg = OauthConfig {
            agent: "x".into(),
            consumer_key: "k".into(),
            consumer_secret: "s".into(),
            callback_url: "/cb".into(),
            cookie_name: "c".into(),
            cookie_secure: true,
            session_lifetime_days: 1,
            session_dir: "/tmp".into(),
            bot_access_key: Some("ak".into()),
            bot_access_secret: None,
        };
        assert!(!cfg.has_bot_credentials());

        let mut cfg2 = cfg.clone();
        cfg2.bot_access_secret = Some("as".into());
        assert!(cfg2.has_bot_credentials());
    }
}
