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
pub fn is_allowed_origin(origin: &str) -> bool {
    let Some(host) = origin.strip_prefix("https://") else {
        return false;
    };
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
}
