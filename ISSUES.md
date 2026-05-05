# Open Issues Tracker

Tracked from [Codeberg issues](https://codeberg.org/magnusmanske/mixnmatch/issues).
Last reviewed: 2026-05-05.

---

## Active Bugs

### [#126 — Low quality automatch results](https://codeberg.org/magnusmanske/mixnmatch/issues/126)
The name/date automatcher produces matches with wrong names and dates off by a century.
Fixing requires tightening scoring thresholds in `src/automatch/`.

### [#100 — Manual sync shows bogus "Multiple external IDs" entries](https://codeberg.org/magnusmanske/mixnmatch/issues/100)
Microsync reports a large number of spurious "multiple external IDs for a single Wikidata item"
entries. Needs a reproducible catalog to diagnose the SPARQL/dedup logic in `src/microsync.rs`.

### [#99 — Unmatched entries contain outdated Wikidata data](https://codeberg.org/magnusmanske/mixnmatch/issues/99)
Items matched in Wikidata (the property is set) still appear as unmatched in MnM.
Microsync isn't pulling those WD→MnM matches into the database after they are created externally.

### [#97 — Language dropdown vanished](https://codeberg.org/magnusmanske/mixnmatch/issues/97)
Users can no longer switch the UI language. Frontend regression; possibly already fixed —
needs verification in the live tool.

### [#93 — Sub-categories ignored in automatch](https://codeberg.org/magnusmanske/mixnmatch/issues/93)
Automatch uses a direct `wdt:P31` check and misses items whose type is a subclass of the
catalog type (e.g. "video game remake" as a subclass of "video game"). Fixing requires
`wdt:P31/wdt:P279*` traversal in `src/automatch/strategies.rs`, but this carries
a SPARQL performance risk.

### [#85 — Creation candidates (human) shows no results](https://codeberg.org/magnusmanske/mixnmatch/issues/85)
`/#/creation_candidates/human` returns "No results, parameters might be too restrictive".
Needs reproduction against a live database.

### [#84 — Simple catalogue import silently truncated](https://codeberg.org/magnusmanske/mixnmatch/issues/84)
A Google Sheets TSV import only imports a few entries instead of the full file.
May be related to the upload size limit (see #83).

### [#83 — Large catalogue import fails silently](https://codeberg.org/magnusmanske/mixnmatch/issues/83)
Catalogs larger than ~20 MB cannot be imported; the upload completes but no new rows appear.
Requires a server-side upload size limit increase (nginx `client_max_body_size` or
equivalent) and/or chunked import support.

### [#71 — Bad links in "Multiple external IDs" sync section](https://codeberg.org/magnusmanske/mixnmatch/issues/71)
Links in the microsync "Multiple external IDs for a single Wikidata item" section point
to non-existent Q-IDs. Likely stale `wd_matches` rows referencing redirected or deleted
items — related to the redirect-resolution work already done for #110.

### [#69 — Manual Sync broken on some catalogs](https://codeberg.org/magnusmanske/mixnmatch/issues/69)
Microsync fails or produces wrong output for certain catalogs. Likely SPARQL timeouts or
catalog misconfiguration. Needs a reproducible catalog to investigate `src/microsync.rs`.

### [#65 — Cannot see anything inside a newly created catalog](https://codeberg.org/magnusmanske/mixnmatch/issues/65)
A freshly created catalog shows no entries. Possibly a display or API race condition;
needs reproduction.

### [#63 — Wrong pronouns in auto-generated descriptions](https://codeberg.org/magnusmanske/mixnmatch/issues/63)
Auto-generated candidate descriptions (e.g. "He was born on …") use the wrong pronoun
for transgender people. The gender text is derived from cached Wikidata P21 data.
Fix options: fetch fresh P21, use gender-neutral wording, or drop pronoun entirely.

### [#51 — Error 413 on large file upload](https://codeberg.org/magnusmanske/mixnmatch/issues/51)
Uploading a TSV file larger than the server's `client_max_body_size` returns HTTP 413.
Server/nginx configuration change required.

### [#48 / #47 — Autoscraper does not handle UTF-8 HTML entities](https://codeberg.org/magnusmanske/mixnmatch/issues/48)
The autoscraper fails to decode UTF-8 HTML entities in scraped content.
May have improved since the 2019 report; needs verification.

---

## Feature Requests

### [#107 — Import-history field + Catalog Update Requests link](https://codeberg.org/magnusmanske/mixnmatch/issues/107)
Add a per-catalog field recording how/when the data was first imported, and link to the
[Catalog Update Requests](https://meta.wikimedia.org/wiki/Talk:Mix%27n%27match#Catalog_Update_Requests)
wiki page.

### [#106 — Auto-unlink scholarly articles from "Preliminarily matched"](https://codeberg.org/magnusmanske/mixnmatch/issues/106)
Items whose P31 is "scholarly article" (or similar non-notable types) should be
automatically unlinked from the "Preliminarily matched" bucket.

### [#103 — Bulk "search Wikidata for all items on page" button](https://codeberg.org/magnusmanske/mixnmatch/issues/103)
A single-click action to trigger an automatch search for all items visible on the
current page.

### [#101 — Extend Mix'n'match gadget to work during Wikidata item creation](https://codeberg.org/magnusmanske/mixnmatch/issues/101)
The on-wiki MnM gadget should also activate on Wikidata's "create new item" form, not
only on existing item pages.

### [#98 — Advanced search / filtering](https://codeberg.org/magnusmanske/mixnmatch/issues/98)
Filter entries by catalog, match state, type, date range, etc. — beyond the current
simple search.

### [#88 — Make scrape parameters visible and testable](https://codeberg.org/magnusmanske/mixnmatch/issues/88)
Scrape configuration for a catalog should be readable by all users and editable by the
catalog creator, not opaque to everyone except admins.

### [#72 — Per-catalog birth/death date range constraints](https://codeberg.org/magnusmanske/mixnmatch/issues/72)
Allow a catalog to specify earliest/latest birth and death years so the date automatcher
ignores implausible candidates.

### [#54 / #43 — Catalog update / dataset refresh flow](https://codeberg.org/magnusmanske/mixnmatch/issues/54)
A proper workflow to update an existing catalog's data (add new rows, update changed
fields) without losing match history.

### [#50 — Pre-matched column in import CSV](https://codeberg.org/magnusmanske/mixnmatch/issues/50)
Allow importers to supply a Q-number column so entries arrive already matched, without
a separate matching pass.

### [#42 — Ability to delete a catalog](https://codeberg.org/magnusmanske/mixnmatch/issues/42)
Catalog owners (or admins) should be able to remove a catalog and all its entries.

### [#26 — Automatch constraints](https://codeberg.org/magnusmanske/mixnmatch/issues/26)
Allow per-catalog constraints on automatching (e.g. "only match items that also have
property P19 set").

### [#24 — Clarify importer format documentation](https://codeberg.org/magnusmanske/mixnmatch/issues/24)
The TSV/CSV import format documentation is ambiguous about column semantics and optional
fields.

### [#21 — Bulk "remove all automatic matches" action](https://codeberg.org/magnusmanske/mixnmatch/issues/21)
A single action to clear all automatcher-set matches for an entry or for a whole page.

### [#19 — Display and use coordinates for geographic items](https://codeberg.org/magnusmanske/mixnmatch/issues/19)
Show a map or coordinates for entries that have lat/lon, and use geographic proximity
as a matching signal.

### [#10 — Log skipped entries per run](https://codeberg.org/magnusmanske/mixnmatch/issues/10)
Record which entries were skipped (and why) during each automatch/autoscrape run so
catalog owners can investigate gaps.

### [#8 — Show date snippet from source in game mode](https://codeberg.org/magnusmanske/mixnmatch/issues/8)
Display a birth/death date excerpt from the external source while the user is playing
the matching game, to aid decision-making.
