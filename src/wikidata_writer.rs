//! `WikidataWriter` trait — abstracts the bot-write surface of `Wikidata`
//! so tests can substitute a mock.
//!
//! Only the four mutating endpoints (`execute_commands`,
//! `set_wikipage_text`, `create_new_wikidata_item`, `perform_ac2wd`)
//! are abstracted here. The read-side surface (`search_api`,
//! `load_sparql_csv`, `get_mw_api`) intentionally stays on the
//! concrete `Wikidata` type:
//!
//! - 22 of ~30 read-side call sites want raw `mediawiki::Api` access.
//! - `mediawiki::Api` is itself constrained to a builder-only API in
//!   the upstream crate, so wrapping it for mock substitution would
//!   reproduce most of the upstream surface.
//!
//! Production code keeps using `app.wikidata_mut().execute_commands(...)`
//! unchanged — `Wikidata` impls this trait, so `&mut Wikidata` coerces
//! to `&mut dyn WikidataWriter` at any boundary that asks for it.
//!
//! Test code can now write:
//! ```ignore
//! let mut writer = MockWikidataWriter::new();
//! my_handler.process(some_input, &mut writer).await?;
//! assert_eq!(writer.commands_for_call(0).len(), 3);
//! ```

use crate::wikidata::Wikidata;
use crate::wikidata_commands::WikidataCommand;
use anyhow::Result;
use async_trait::async_trait;
#[cfg(test)]
use wikimisc::wikibase::EntityTrait;

/// Bot-write surface of Wikidata. Implemented by [`Wikidata`] for
/// production and by `MockWikidataWriter` (test-only) for unit tests.
#[async_trait]
pub trait WikidataWriter: std::fmt::Debug + Send + Sync {
    /// Send a batch of structured edits (claim adds, removes, etc.) to Wikidata.
    async fn execute_commands(&mut self, commands: Vec<WikidataCommand>) -> Result<()>;

    /// Edit a wiki page's full wikitext (used by `microsync` to write the
    /// "items needing review" subpage on the bot user's userspace).
    async fn set_wikipage_text(
        &mut self,
        title: &str,
        wikitext: &str,
        summary: &str,
    ) -> Result<()>;

    /// `wbeditentity new=item` — create a Wikidata item from an
    /// `ItemEntity`. Returns the new QID.
    async fn create_new_wikidata_item(
        &mut self,
        item: &wikimisc::wikibase::ItemEntity,
        comment: &str,
    ) -> Result<String>;

    /// AC2WD authority-control extension — calls
    /// `https://ac2wd.toolforge.org/extend/{q}` and applies the returned
    /// patch to the existing item via `wbeditentity`.
    async fn perform_ac2wd(&mut self, q: &str) -> Result<String>;
}

#[async_trait]
impl WikidataWriter for Wikidata {
    async fn execute_commands(&mut self, commands: Vec<WikidataCommand>) -> Result<()> {
        // Concrete-type path resolves to the inherent method on Wikidata
        // (Rust's method resolution prefers inherent over trait when the
        // path is a concrete type), so this is *not* a recursive call.
        Wikidata::execute_commands(self, commands).await
    }

    async fn set_wikipage_text(
        &mut self,
        title: &str,
        wikitext: &str,
        summary: &str,
    ) -> Result<()> {
        Wikidata::set_wikipage_text(self, title, wikitext, summary).await
    }

    async fn create_new_wikidata_item(
        &mut self,
        item: &wikimisc::wikibase::ItemEntity,
        comment: &str,
    ) -> Result<String> {
        Wikidata::create_new_wikidata_item(self, item, comment).await
    }

    async fn perform_ac2wd(&mut self, q: &str) -> Result<String> {
        Wikidata::perform_ac2wd(self, q).await
    }
}

/// Test double for [`WikidataWriter`]. Records every call so unit tests
/// can assert on what their handler tried to send to Wikidata, without
/// any network access. Public so callers in other modules can build
/// their own focused tests; the recorded fields are read-only after a
/// call.
#[cfg(test)]
#[derive(Debug, Default)]
pub struct MockWikidataWriter {
    /// One entry per `execute_commands` call. Cloned in so the test can
    /// re-inspect after the handler finishes.
    pub execute_calls: Vec<Vec<WikidataCommand>>,
    /// `(title, wikitext, summary)` per `set_wikipage_text` call.
    pub set_wikipage_calls: Vec<(String, String, String)>,
    /// `(item_json, comment)` per `create_new_wikidata_item` call. We
    /// store the JSON form to avoid having to clone the full
    /// `ItemEntity` (which doesn't impl `Clone` consistently across
    /// versions of `wikimisc`).
    pub create_calls: Vec<(serde_json::Value, String)>,
    /// QIDs passed to `perform_ac2wd`.
    pub ac2wd_calls: Vec<String>,
    /// If set, the next mutating call returns this error instead of
    /// `Ok(())`. Used to exercise error-handling paths in callers.
    pub next_error: Option<String>,
    /// Optional override for what `create_new_wikidata_item` and
    /// `perform_ac2wd` return. Default is `"Q-MOCK-1"`.
    pub next_qid: Option<String>,
}

#[cfg(test)]
impl MockWikidataWriter {
    pub fn new() -> Self {
        Self::default()
    }

    fn take_error(&mut self) -> Option<anyhow::Error> {
        self.next_error.take().map(anyhow::Error::msg)
    }

    fn take_qid(&mut self) -> String {
        self.next_qid
            .take()
            .unwrap_or_else(|| "Q-MOCK-1".to_string())
    }
}

#[cfg(test)]
#[async_trait]
impl WikidataWriter for MockWikidataWriter {
    async fn execute_commands(&mut self, commands: Vec<WikidataCommand>) -> Result<()> {
        if let Some(e) = self.take_error() {
            return Err(e);
        }
        self.execute_calls.push(commands);
        Ok(())
    }

    async fn set_wikipage_text(
        &mut self,
        title: &str,
        wikitext: &str,
        summary: &str,
    ) -> Result<()> {
        if let Some(e) = self.take_error() {
            return Err(e);
        }
        self.set_wikipage_calls
            .push((title.to_string(), wikitext.to_string(), summary.to_string()));
        Ok(())
    }

    async fn create_new_wikidata_item(
        &mut self,
        item: &wikimisc::wikibase::ItemEntity,
        comment: &str,
    ) -> Result<String> {
        if let Some(e) = self.take_error() {
            return Err(e);
        }
        self.create_calls.push((item.to_json(), comment.to_string()));
        Ok(self.take_qid())
    }

    async fn perform_ac2wd(&mut self, q: &str) -> Result<String> {
        if let Some(e) = self.take_error() {
            return Err(e);
        }
        self.ac2wd_calls.push(q.to_string());
        Ok(self.take_qid())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_records_execute_commands() {
        let mut w = MockWikidataWriter::new();
        // Two batches.
        WikidataWriter::execute_commands(&mut w, vec![]).await.unwrap();
        WikidataWriter::execute_commands(&mut w, vec![]).await.unwrap();
        assert_eq!(w.execute_calls.len(), 2);
    }

    #[tokio::test]
    async fn mock_records_set_wikipage_args() {
        let mut w = MockWikidataWriter::new();
        WikidataWriter::set_wikipage_text(&mut w, "T", "wt", "s")
            .await
            .unwrap();
        assert_eq!(w.set_wikipage_calls, vec![("T".into(), "wt".into(), "s".into())]);
    }

    #[tokio::test]
    async fn mock_returns_configured_qid_for_ac2wd() {
        let mut w = MockWikidataWriter::new();
        w.next_qid = Some("Q42".into());
        let got = WikidataWriter::perform_ac2wd(&mut w, "Q1").await.unwrap();
        assert_eq!(got, "Q42");
        assert_eq!(w.ac2wd_calls, vec!["Q1".to_string()]);
    }

    #[tokio::test]
    async fn mock_propagates_configured_error() {
        let mut w = MockWikidataWriter::new();
        w.next_error = Some("boom".into());
        let err = WikidataWriter::execute_commands(&mut w, vec![])
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "boom");
        // Error consumed; next call succeeds.
        WikidataWriter::execute_commands(&mut w, vec![]).await.unwrap();
        assert_eq!(w.execute_calls.len(), 1);
    }

    /// Compile-time check: `&mut Wikidata` can be passed where
    /// `&mut dyn WikidataWriter` is expected. Doesn't actually
    /// run — `get_test_app()` would need a config.json — but
    /// proves the dyn dispatch compiles.
    #[allow(dead_code)]
    fn coerces_to_dyn(wd: &mut Wikidata) {
        fn takes_writer(_: &mut dyn WikidataWriter) {}
        takes_writer(wd);
    }
}
