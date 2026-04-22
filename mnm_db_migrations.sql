-- Indexes that speed up hot read paths. Safe to apply as ALTER TABLE …
-- ALGORITHM=INPLACE, LOCK=NONE on modern MySQL / MariaDB; run outside
-- a release window regardless because the entry table is very large.
--
-- Each block is idempotent: the `IF NOT EXISTS` clause makes re-running
-- the file a no-op. That's not a full cross-version MySQL feature — on
-- older server versions you'd need to drop the clause and guard with
-- `SELECT COUNT(*) FROM information_schema.statistics WHERE …` instead.

-- ---------------------------------------------------------------
-- 1) entry(catalog, q)
--
-- Drives `query=catalog` with `show_noq` / `show_na` / `show_nowd` only
-- (the URLs the frontend `catalog_list.js` builds for the
-- Unmatched / N-A / No-Wikidata tabs). The existing indexes are
-- `catalog_only(catalog)`, `catalog_2(catalog, type)`, and
-- `catalog_user(catalog, user)` — none of which let MySQL narrow on
-- `q IS NULL` without walking every row in the catalog.
-- ---------------------------------------------------------------
ALTER TABLE `entry`
    ADD INDEX IF NOT EXISTS `catalog_q` (`catalog`, `q`);

-- ---------------------------------------------------------------
-- 2) entry(user, timestamp)
--
-- Speeds up `query=rc` with a time filter (`ts` ≥ some cutoff) and
-- `query=user_edits` (always filtered on `user`). The existing index
-- `timestamp(timestamp)` sorts but doesn't filter on user, and
-- `user(user)` filters but doesn't sort — the composite gives a
-- reverse-scan range that terminates as soon as the LIMIT is met.
-- ---------------------------------------------------------------
ALTER TABLE `entry`
    ADD INDEX IF NOT EXISTS `user_timestamp` (`user`, `timestamp`);
