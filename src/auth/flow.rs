use crate::auth::config::{MW_API_URL, MW_OAUTH_BASE_URL, OauthConfig};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use rand::RngExt;
use reqwest::Url;
use serde::Deserialize;
use sha1::Sha1;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha1 = Hmac<Sha1>;

#[derive(Clone)]
pub struct TokenPair {
    pub key: String,
    pub secret: String,
}

impl std::fmt::Debug for TokenPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never leak token secrets via Debug.
        f.debug_struct("TokenPair")
            .field("key", &self.key)
            .field("secret", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct WikidataUser {
    pub id: u64,
    pub name: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    key: String,
    secret: String,
}

#[derive(Deserialize)]
struct TokenError {
    error: Option<String>,
    message: Option<String>,
}

/// Fetch a fresh request token from `Special:OAuth/initiate`.
/// PHP equivalent: `MW_OAuth::doAuthorizationRedirect` (the first half).
pub async fn initiate_request_token(cfg: &OauthConfig) -> Result<TokenPair> {
    let endpoint = format!("{MW_OAUTH_BASE_URL}/initiate");
    let mut params = base_oauth_params(cfg);
    params.push(("format".to_string(), "json".to_string()));
    params.push(("oauth_callback".to_string(), cfg.callback_url.clone()));

    let body = signed_get(cfg, &endpoint, params, "", "").await?;
    parse_token_response(&body)
}

/// URL the user is redirected to in order to grant access.
/// PHP equivalent: the `Location:` header at the end of `doAuthorizationRedirect`.
pub fn build_authorize_redirect_url(cfg: &OauthConfig, request_token_key: &str) -> String {
    let mut pairs: [(String, String); 2] = [
        ("oauth_token".to_string(), request_token_key.to_string()),
        ("oauth_consumer_key".to_string(), cfg.consumer_key.clone()),
    ];
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    let query = pairs
        .iter()
        .map(|(k, v)| format!("{}={}", rfc3986_encode(k), rfc3986_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{MW_OAUTH_BASE_URL}/authorize?{query}")
}

/// Exchange the (request token, verifier) for an access token.
/// PHP equivalent: `MW_OAuth::fetchAccessToken`.
pub async fn exchange_verifier(
    cfg: &OauthConfig,
    request_token: &TokenPair,
    verifier: &str,
) -> Result<TokenPair> {
    let endpoint = format!("{MW_OAUTH_BASE_URL}/token");
    let mut params = base_oauth_params(cfg);
    params.push(("format".to_string(), "json".to_string()));
    params.push(("oauth_verifier".to_string(), verifier.to_string()));
    params.push(("oauth_token".to_string(), request_token.key.clone()));

    let body = signed_get(cfg, &endpoint, params, &request_token.key, &request_token.secret).await?;
    parse_token_response(&body)
}

/// Fetch `meta=userinfo` on the editing wiki, signed with the access token.
/// PHP equivalent: `MW_OAuth::doApiQuery(['action'=>'query','meta'=>'userinfo'])`,
/// which is how `Widar::get_username` resolves the logged-in user.
pub async fn fetch_userinfo(cfg: &OauthConfig, access: &TokenPair) -> Result<WikidataUser> {
    let mut params = base_oauth_params(cfg);
    params.push(("oauth_token".to_string(), access.key.clone()));
    params.push(("format".to_string(), "json".to_string()));
    params.push(("action".to_string(), "query".to_string()));
    params.push(("meta".to_string(), "userinfo".to_string()));

    let body = signed_get(cfg, MW_API_URL, params, &access.key, &access.secret).await?;
    parse_userinfo_response(&body)
}

// ---------------------------------------------------------------------------
// Low-level: signed GET
// ---------------------------------------------------------------------------

async fn signed_get(
    cfg: &OauthConfig,
    url: &str,
    mut params: Vec<(String, String)>,
    token_key: &str,
    token_secret: &str,
) -> Result<String> {
    if !token_key.is_empty() {
        // Already pushed by the caller when we want it as a parameter, but the
        // PHP `sign_request` reads `gTokenSecret` from the instance, not the
        // caller. We never add `oauth_token` twice — callers that need it
        // include it already.
        let has = params.iter().any(|(k, _)| k == "oauth_token");
        if !has {
            params.push(("oauth_token".to_string(), token_key.to_string()));
        }
    }

    let signature = sign_request("GET", url, &params, &cfg.consumer_secret, token_secret);
    params.push(("oauth_signature".to_string(), signature));

    let final_url = build_query_url(url, &params);
    let client = reqwest::Client::new();
    let resp = client
        .get(&final_url)
        .header(reqwest::header::USER_AGENT, &cfg.agent)
        .send()
        .await
        .map_err(|e| anyhow!("OAuth request failed: {e}"))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| anyhow!("OAuth response read failed: {e}"))?;
    if !status.is_success() {
        return Err(anyhow!(
            "OAuth endpoint returned HTTP {status}: {}",
            truncate_for_log(&body)
        ));
    }
    Ok(body)
}

fn base_oauth_params(cfg: &OauthConfig) -> Vec<(String, String)> {
    vec![
        ("oauth_consumer_key".to_string(), cfg.consumer_key.clone()),
        ("oauth_version".to_string(), "1.0".to_string()),
        ("oauth_nonce".to_string(), random_nonce()),
        ("oauth_timestamp".to_string(), unix_timestamp()),
        ("oauth_signature_method".to_string(), "HMAC-SHA1".to_string()),
    ]
}

fn random_nonce() -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 16] = std::array::from_fn(|_| rng.random());
    // Hex is fine; the spec just requires it be unique within the request's timestamp.
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unix_timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Signature — exact parity with PHP MW_OAuth::sign_request
// ---------------------------------------------------------------------------

fn sign_request(
    method: &str,
    url: &str,
    params: &[(String, String)],
    consumer_secret: &str,
    token_secret: &str,
) -> String {
    let parsed = Url::parse(url).expect("OAuth URLs are hard-coded valid");
    let scheme = parsed.scheme();
    let host = parsed.host_str().unwrap_or("");
    let port = parsed.port().unwrap_or(match scheme {
        "https" => 443,
        _ => 80,
    });
    let path = parsed.path();

    let default_port = matches!((scheme, port), ("https", 443) | ("http", 80));
    let host_with_port = if default_port {
        host.to_string()
    } else {
        format!("{host}:{port}")
    };
    let base_url = format!("{scheme}://{host_with_port}{path}");

    // Collect parameters: URL query + caller-provided params, minus oauth_signature.
    let mut all: Vec<(String, String)> = parsed
        .query_pairs()
        .filter(|(k, _)| k != "oauth_signature")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    for (k, v) in params {
        if k == "oauth_signature" {
            continue;
        }
        all.push((k.clone(), v.clone()));
    }

    // rfc3986-encode both key and value, then sort.
    let mut encoded: Vec<(String, String)> = all
        .into_iter()
        .map(|(k, v)| (rfc3986_encode(k.as_str()), rfc3986_encode(v.as_str())))
        .collect();
    encoded.sort();
    let joined = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let to_sign = format!(
        "{}&{}&{}",
        rfc3986_encode(&method.to_uppercase()),
        rfc3986_encode(&base_url),
        rfc3986_encode(&joined)
    );
    let key = format!(
        "{}&{}",
        rfc3986_encode(consumer_secret),
        rfc3986_encode(token_secret)
    );

    let mut mac = HmacSha1::new_from_slice(key.as_bytes())
        .expect("HMAC-SHA1 accepts any key length");
    mac.update(to_sign.as_bytes());
    let digest = mac.finalize().into_bytes();
    base64::engine::general_purpose::STANDARD.encode(digest)
}

/// rawurlencode-compatible encoding: unreserved characters per RFC 3986.
/// `A-Z a-z 0-9 - _ . ~` are preserved, everything else is percent-encoded.
/// Matches PHP `rawurlencode` byte-for-byte.
fn rfc3986_encode(s: &str) -> String {
    const UNRESERVED: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
    let mut out = String::with_capacity(s.len());
    for &byte in s.as_bytes() {
        if UNRESERVED.contains(&byte) {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn build_query_url(base: &str, params: &[(String, String)]) -> String {
    let query = params
        .iter()
        .map(|(k, v)| format!("{}={}", rfc3986_encode(k), rfc3986_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let sep = if base.contains('?') { '&' } else { '?' };
    format!("{base}{sep}{query}")
}

// ---------------------------------------------------------------------------
// Response parsing
// ---------------------------------------------------------------------------

fn parse_token_response(body: &str) -> Result<TokenPair> {
    if let Ok(err) = serde_json::from_str::<TokenError>(body) {
        if let Some(msg) = err.error.or(err.message) {
            return Err(anyhow!("OAuth token error: {msg}"));
        }
    }
    let token: TokenResponse = serde_json::from_str(body).map_err(|e| {
        anyhow!(
            "Cannot parse OAuth token response: {e}. Body: {}",
            truncate_for_log(body)
        )
    })?;
    if token.key.is_empty() || token.secret.is_empty() {
        return Err(anyhow!(
            "OAuth token response missing key or secret: {}",
            truncate_for_log(body)
        ));
    }
    Ok(TokenPair {
        key: token.key,
        secret: token.secret,
    })
}

fn parse_userinfo_response(body: &str) -> Result<WikidataUser> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| anyhow!("Cannot parse userinfo JSON: {e}. Body: {}", truncate_for_log(body)))?;
    if let Some(err) = v.get("error") {
        return Err(anyhow!("OAuth userinfo error: {err}"));
    }
    let info = v
        .pointer("/query/userinfo")
        .ok_or_else(|| anyhow!("userinfo missing /query/userinfo"))?;
    if info.get("anon").is_some() {
        return Err(anyhow!("userinfo reports an anonymous session"));
    }
    let id = info
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("userinfo missing numeric id"))?;
    let name = info
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("userinfo missing name"))?
        .to_string();
    Ok(WikidataUser { id, name })
}

fn truncate_for_log(s: &str) -> String {
    if s.len() > 400 {
        format!("{}…", &s[..400])
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Tests — signature parity with PHP
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3986_matches_rawurlencode() {
        // Spot checks corresponding to what PHP's rawurlencode produces.
        assert_eq!(rfc3986_encode("abc"), "abc");
        assert_eq!(rfc3986_encode("a b"), "a%20b");
        assert_eq!(rfc3986_encode("a+b"), "a%2Bb");
        assert_eq!(rfc3986_encode("~."), "~.");
        assert_eq!(rfc3986_encode("Hellö"), "Hell%C3%B6");
        assert_eq!(rfc3986_encode("&=?/"), "%26%3D%3F%2F");
    }

    #[test]
    fn build_authorize_url_shape() {
        let cfg = OauthConfig {
            agent: "t".into(),
            consumer_key: "CK".into(),
            consumer_secret: "CS".into(),
            callback_url: "http://x/cb".into(),
            cookie_name: "c".into(),
            cookie_secure: false,
            session_lifetime_days: 30,
            session_dir: "./sessions".into(),
        };
        let url = build_authorize_redirect_url(&cfg, "RTK");
        assert!(url.starts_with("https://www.mediawiki.org/wiki/Special:OAuth/authorize?"));
        assert!(url.contains("oauth_token=RTK"));
        assert!(url.contains("oauth_consumer_key=CK"));
    }

    /// Known-good signature vector: compute the signature for a fixed request
    /// and verify it matches what PHP's sign_request would produce. Reference:
    /// OAuth 1.0a Test Cases (RFC 5849 §A.5) adapted to our base-URL builder.
    #[test]
    fn sign_request_matches_rfc5849_example() {
        // From RFC 5849 §A.5 (example request to a demo endpoint). The expected
        // signature was computed with PHP's sign_request using identical inputs
        // and cross-checked against the RFC value.
        let params = vec![
            ("oauth_consumer_key".to_string(), "dpf43f3p2l4k3l03".to_string()),
            ("oauth_token".to_string(), "nnch734d00sl2jdk".to_string()),
            ("oauth_nonce".to_string(), "kllo9940pd9333jh".to_string()),
            ("oauth_timestamp".to_string(), "1191242096".to_string()),
            ("oauth_signature_method".to_string(), "HMAC-SHA1".to_string()),
            ("oauth_version".to_string(), "1.0".to_string()),
            ("file".to_string(), "vacation.jpg".to_string()),
            ("size".to_string(), "original".to_string()),
        ];
        let sig = sign_request(
            "GET",
            "http://photos.example.net/photos",
            &params,
            "kd94hf93k423kf44",
            "pfkkdhi9sl3r4s00",
        );
        assert_eq!(sig, "tR3+Ty81lMeYAr/Fid0kMTYa/WM=");
    }
}
