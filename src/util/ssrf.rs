//! SSRF guard for outbound HTTP from autoscrape / import / scraper-test
//! endpoints. Hooked into `reqwest::ClientBuilder::dns_resolver` so the
//! filter survives redirects (reqwest re-resolves on each hop) without
//! needing per-call URL inspection at the call sites.
//!
//! The predicate ([`is_blocked_ip`]) is split from the resolver so unit
//! tests don't need DNS or an async runtime: they exercise the IP
//! classification directly. The audit reference is H-2 in
//! `audits/comprehensive_security_report.md`.

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// True for any IP we refuse to fetch from: loopback, private, link-local
/// (catches the AWS / WMCS metadata IP `169.254.169.254`), CGNAT, benchmark,
/// documentation, reserved, multicast, and unspecified ranges. IPv6 covers
/// the equivalent ranges plus `::ffff:0:0/96` IPv4-mapped addresses (so an
/// attacker can't smuggle a private IPv4 inside an AAAA response).
#[must_use]
pub fn is_blocked_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_blocked_ipv4(&mapped);
            }
            is_blocked_ipv6(v6)
        }
    }
}

fn is_blocked_ipv4(ip: &Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_unspecified()        // 0.0.0.0/8
        || ip.is_loopback()    // 127.0.0.0/8
        || ip.is_private()     // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()  // 169.254.0.0/16 — includes cloud metadata
        || ip.is_broadcast()
        || ip.is_documentation() // 192.0.2/24, 198.51.100/24, 203.0.113/24
        || ip.is_multicast()
        // RFC 6598 carrier-grade NAT: 100.64.0.0/10
        || (o[0] == 100 && (o[1] & 0b1100_0000) == 0b0100_0000)
        // RFC 2544 benchmark: 198.18.0.0/15
        || (o[0] == 198 && (o[1] & 0b1111_1110) == 18)
        // Reserved: 240.0.0.0/4 (excluding broadcast handled above)
        || o[0] >= 240
}

fn is_blocked_ipv6(ip: &Ipv6Addr) -> bool {
    let s = ip.segments();
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        // Unique local fc00::/7
        || (s[0] & 0xfe00) == 0xfc00
        // Link-local fe80::/10
        || (s[0] & 0xffc0) == 0xfe80
}

/// `reqwest`-compatible DNS resolver that drops any address in a blocked
/// range. If every resolved address is blocked the call returns an error
/// and reqwest surfaces it as a connect failure — same UX as a hostname
/// that genuinely doesn't exist.
#[derive(Debug, Default, Clone, Copy)]
pub struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();
            // Port 0 is fine — reqwest only uses the IP component of each
            // resolved address; it supplies its own port from the request URL.
            let addrs: Vec<SocketAddr> =
                match tokio::net::lookup_host((host.as_str(), 0_u16)).await {
                    Ok(iter) => iter.collect(),
                    Err(e) => {
                        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
                    }
                };
            let filtered: Vec<SocketAddr> = addrs
                .into_iter()
                .filter(|sa| !is_blocked_ip(&sa.ip()))
                .collect();
            if filtered.is_empty() {
                let msg = format!(
                    "SSRF guard: '{host}' resolves only to blocked (private/loopback/link-local/reserved) addresses"
                );
                return Err(msg.into());
            }
            let iter: Addrs = Box::new(filtered.into_iter());
            Ok(iter)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("test IP must parse")
    }

    #[test]
    fn blocks_loopback_v4() {
        assert!(is_blocked_ip(&ip("127.0.0.1")));
        assert!(is_blocked_ip(&ip("127.255.255.255")));
    }

    #[test]
    fn blocks_link_local_including_cloud_metadata() {
        assert!(is_blocked_ip(&ip("169.254.0.1")));
        // AWS / GCP / Azure / WMCS instance metadata
        assert!(is_blocked_ip(&ip("169.254.169.254")));
    }

    #[test]
    fn blocks_rfc1918_private_v4() {
        assert!(is_blocked_ip(&ip("10.0.0.1")));
        assert!(is_blocked_ip(&ip("10.255.255.255")));
        assert!(is_blocked_ip(&ip("172.16.0.1")));
        assert!(is_blocked_ip(&ip("172.31.255.255")));
        assert!(is_blocked_ip(&ip("192.168.1.1")));
    }

    #[test]
    fn blocks_unspecified_broadcast_multicast_documentation() {
        assert!(is_blocked_ip(&ip("0.0.0.0")));
        assert!(is_blocked_ip(&ip("255.255.255.255")));
        assert!(is_blocked_ip(&ip("224.0.0.1")));
        assert!(is_blocked_ip(&ip("192.0.2.1")));
        assert!(is_blocked_ip(&ip("198.51.100.1")));
        assert!(is_blocked_ip(&ip("203.0.113.1")));
    }

    #[test]
    fn blocks_cgnat_benchmark_reserved() {
        assert!(is_blocked_ip(&ip("100.64.0.1")));
        assert!(is_blocked_ip(&ip("100.127.255.255")));
        // Just outside CGNAT — must be allowed
        assert!(!is_blocked_ip(&ip("100.63.255.255")));
        assert!(!is_blocked_ip(&ip("100.128.0.0")));
        // Benchmark 198.18.0.0/15
        assert!(is_blocked_ip(&ip("198.18.0.1")));
        assert!(is_blocked_ip(&ip("198.19.255.255")));
        // Reserved 240/4
        assert!(is_blocked_ip(&ip("240.0.0.1")));
        assert!(is_blocked_ip(&ip("250.0.0.1")));
    }

    #[test]
    fn allows_public_v4() {
        assert!(!is_blocked_ip(&ip("8.8.8.8")));
        assert!(!is_blocked_ip(&ip("1.1.1.1")));
        // Wikidata's typical resolved address space
        assert!(!is_blocked_ip(&ip("208.80.154.224")));
    }

    #[test]
    fn blocks_loopback_and_local_v6() {
        assert!(is_blocked_ip(&ip("::1")));
        assert!(is_blocked_ip(&ip("::")));
        assert!(is_blocked_ip(&ip("fe80::1")));
        assert!(is_blocked_ip(&ip("fc00::1")));
        assert!(is_blocked_ip(&ip("fd00::1")));
        assert!(is_blocked_ip(&ip("ff02::1")));
    }

    #[test]
    fn blocks_ipv4_mapped_ipv6() {
        // ::ffff:127.0.0.1 — IPv4-mapped form of a loopback. Without
        // the to_ipv4_mapped() check this would slip through.
        assert!(is_blocked_ip(&ip("::ffff:127.0.0.1")));
        assert!(is_blocked_ip(&ip("::ffff:10.0.0.1")));
        assert!(is_blocked_ip(&ip("::ffff:169.254.169.254")));
    }

    #[test]
    fn allows_public_v6() {
        // Google public DNS
        assert!(!is_blocked_ip(&ip("2001:4860:4860::8888")));
        // Wikidata
        assert!(!is_blocked_ip(&ip("2620:0:861:ed1a::1")));
    }

    /// End-to-end: hand the resolver a real localhost name and confirm
    /// it refuses (loopback addrs are filtered). Uses tokio's resolver
    /// under the hood so it covers the same path reqwest will take.
    #[tokio::test]
    async fn resolver_refuses_localhost() {
        let r = PublicOnlyResolver;
        let name: Name = "localhost".parse().expect("valid hostname");
        let res = r.resolve(name).await;
        assert!(
            res.is_err(),
            "PublicOnlyResolver must refuse 'localhost' — got Ok"
        );
        let msg = res.err().unwrap().to_string();
        assert!(
            msg.contains("SSRF guard"),
            "error message should identify the SSRF guard, got: {msg}"
        );
    }
}
