#![allow(clippy::mod_module_files)]

//! OAuth1.0a login and session handling for the public web server.
//!
//! Mirrors the semantics of the PHP `Widar` / `MW_OAuth` stack: a user signs in
//! by redirecting to `Special:OAuth/authorize` on Wikidata; after the callback
//! the access token is stored server-side in a session store, keyed by a cookie.
//! Every write action on `/api.php` is attributed to the *verified* session
//! username, never to whatever the client put in the `username` form field.

pub mod config;
pub mod flow;
pub mod guard;
pub mod session;
