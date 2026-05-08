//! CORS origin allowlist. Shared between the `tower_http::cors` layer (which
//! gates preflights and adds response headers) and the dispatcher's CSRF
//! guard (which blocks simple cross-origin GETs that CORS can't stop
//! server-side).

const ALLOWED_SUFFIXES: &[&str] = &[
    "wikidata.org",
    "wikipedia.org",
    "wikimedia.org",
    "wiktionary.org",
    "wikibooks.org",
    "wikiquote.org",
    "wikinews.org",
    "wikisource.org",
    "wikiversity.org",
    "wikivoyage.org",
    "toolforge.org",
];

/// True if `origin` is `https://<host>` where `host` is, or is a subdomain of,
/// one of the allowed Wikimedia / Toolforge project domains.
/// Off-Toolforge (local dev) also allows `localhost` and `127.0.0.1` origins on
/// any port, matching the OAuth dev-bypass in `auth::guard`.
pub fn is_allowed_origin(origin: &str) -> bool {
    let Some(host) = origin.strip_prefix("https://") else {
        return false;
    };
    if !crate::app_state::is_on_toolforge() {
        let bare = host.split(':').next().unwrap_or(host);
        if bare == "localhost" || bare == "127.0.0.1" || bare == "::1" {
            return true;
        }
    }
    ALLOWED_SUFFIXES
        .iter()
        .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_wikimedia_origins() {
        assert!(is_allowed_origin("https://www.wikidata.org"));
        assert!(is_allowed_origin("https://wikidata.org"));
        assert!(is_allowed_origin("https://en.wikipedia.org"));
        assert!(is_allowed_origin("https://commons.wikimedia.org"));
        assert!(is_allowed_origin("https://mix-n-match.toolforge.org"));
    }

    #[test]
    fn rejects_http_and_non_wikimedia() {
        assert!(!is_allowed_origin("http://www.wikidata.org")); // http, not https
        assert!(!is_allowed_origin("https://evil.com"));
        assert!(!is_allowed_origin("https://wikidata.org.evil.com"));
        assert!(!is_allowed_origin("https://evilwikidata.org")); // not a suffix match
    }

    #[test]
    fn allows_localhost_when_not_on_toolforge() {
        // This test only exercises the non-Toolforge path; on Toolforge CI it
        // would need /etc/wmcs-project to exist, so we guard it.
        if crate::app_state::is_on_toolforge() {
            return;
        }
        assert!(is_allowed_origin("https://localhost:8080"));
        assert!(is_allowed_origin("https://127.0.0.1:8080"));
        assert!(is_allowed_origin("https://127.0.0.1"));
        assert!(!is_allowed_origin("http://localhost:8080")); // http still rejected
    }
}
