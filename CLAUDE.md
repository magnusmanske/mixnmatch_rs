# Mix'n'match (Rust) — Claude project notes

## Hard rules

- **Do not add the Claude attribution to commit messages.** No `Co-Authored-By: Claude …` line.
- **`git commit` is pre-authorized; `git push` is not.** When work is complete and the diff is reviewable, you may run `git add <specific files>` + `git commit` without asking — this is durable authorization. **Never `git push`, `git push --force`, or otherwise publish commits to a remote without an explicit per-action request from the user.** Same for any other remote-publishing action (PR creation, branch deletion on origin, etc.). Use sensible commit groupings for larger changes.
- **Never edit `/Users/magnusmanske/php/mixnmatch`.** That's the PHP companion repo. The frontend for this Rust tree lives at `html/` inside this repo.
- **`audits/`, `CLAUDE.md`, `config.json`, `oauth.ini`, `flickr.key`, `.claude/` are all gitignored.** Don't try to git-add them; treat audit files as durable scratch.
- **Never alter the production database**, no additions, alterations, deletions, unless specifically told to.
- **`config.json` is required at runtime.** Tests that need it are `#[ignore]`-gated (see *Testing* below).
- API actions that change the database state should always be guarded by **OAuth login**.
- This is a web-facing product, so **keep security in mind**.
- Always keep **code readability and long-term maintenance** in mind.
- Adhere to **SOLID and DRY principles**.
- Use **best practices** and **language standards**.
- **Keep the code simple** and elegant.
- **Write tests** where it makes sense.
- **Aim to keep code small** where possible.
- **All custom CSS belongs in `html/main.css`.** Do not inject styles via `document.createElement('style')` in Vue components or any other JS file. Add a clearly labelled section (`/* ========== Component name ========== */`) to `main.css` instead.
- Fix clippy warnings.

## What this is

Seb server and background (bot) tasks for [Mix'n'match](https://mix-n-match.toolforge.org/), a Wikidata reconciliation tool. ~47 k LOC, ~100 files.

## Layout

```
src/
  app_state.rs            AppState (runtime root) + AppContext trait facade
  api/                    HTTP API; api/router.rs holds the `ROUTES` table
  auth/                   OAuth flow
  automatch/              {mod, strategies, dates}.rs — search/label/date matchers
  auxiliary_matcher/      {mod, finder, sync}.rs — aux→WD discovery + push
  bespoke_scrapers/       per-catalog scrapers; mod.rs holds SCRAPER_REGISTRY
  bin/                    binaries (main.rs)
  code_fragment.rs        Lua VM + per-catalog code fragment runners
  entry.rs                domain model + repo + WD writer (large; see audit)
  job.rs                  Job + Jobbable trait + 3 dispatchers
  maintenance/            {mod, cleanup, wikidata_sync}.rs
  storage.rs              Storage trait (~188 methods) + sub-traits
  storage_mysql/          {mod, builders, row_mappers, util}.rs — only real impl
  storage_wikibase.rs     DEAD CODE — `mod` decl in lib.rs:93 is commented out
  util/wikidata_props.rs  P31, P569, etc. — use these, not magic strings
audits/                   gitignored long-form findings (complexity, duplication, SOLID)
html/                     frontend (don't move; some live-served paths assume it)
```

## Build & test

```bash
cargo build                    # clean as of last commit
cargo test --lib               # full suite (~1424 tests, ~30 s on a fast machine,
                               # ~2-3 min on slow Docker — boots a MariaDB testcontainer)
cargo test-fast                # ~1057 pure-logic tests, ~2 s — skips every module
                               # whose tests need the MariaDB container. Use for
                               # local iteration on non-DB changes. Defined in
                               # `.cargo/config.toml`; the skip list is the
                               # authoritative reference for which modules are
                               # "DB-heavy".
cargo test -- --ignored        # DB- / network-dependent tests; needs config.json
```

Two existing tests (`wikidata::tests::test_wd_search`, `microsync::tests::test_get_formatter_url_for_prop`) flake under concurrent load — they hit live Wikidata APIs and get rate-limited when the full suite runs them in parallel. Pass when run individually. **Not regressions.**

The container test fixture starts MariaDB once per test process (`test_support::TEST_DB`, a `OnceCell`). Cold-start cost is ~60 s on a typical machine, longer under Docker contention. `cargo test --lib` amortises this against parallel pure-logic test execution; `cargo test-fast` avoids it entirely by skipping every module that calls `test_support::test_app()` / `seed_*()`.

## Architecture cheat sheet

- **`AppState`** (`src/app_state.rs`) is the runtime root: storage handle, two `Wikidata` clients (`wikidata` + `wdt` for the terms replica), `wdrc`, shared `reqwest::Client`, config, OAuth. Imported by ~55 files. **`AppContext`** trait (same file) is the read-only view; new code should prefer `&impl AppContext` over `&AppState` (see `audits/code_solid.md` for migration plan).
- **`Storage` trait** (`src/storage.rs`) is the DB facade. Currently ~188 methods on the umbrella + 8 in carved-out sub-traits (`IssueQueries`, `CoordinateMatcherQueries`). Real impl: `StorageMySQL` only. Trait upcasting (Rust 1.86+) keeps `&dyn Storage` callers happy when sub-traits are added.
- **Job system** (`src/job.rs`): `Job::run_this_job` → `dispatch_automatch` / `dispatch_maintenance` / `dispatch_other`. Each takes one `&str` action and routes to the matching subsystem. Adding a new action = one match arm in the right helper.
- **Strategies pattern**: `AutoMatch`, `Maintenance`, `AuxiliaryMatcher` are stateful structs holding `app: AppState` + `job: Option<Job>`. The `Jobbable` trait (in `job.rs`) provides offset bookkeeping. They're constructed per-job-run; cloning `AppState` is cheap (internal `Arc`s).
- **Bespoke scrapers**: `BespokeScraper` trait isn't object-safe (returns `Self` in `new`), so `bespoke_scrapers.rs` uses `SCRAPER_REGISTRY: &[(usize, ScraperRunFn)]` of `BoxFuture`-returning fn-pointers. Add a scraper = add `pub mod scraper_X` + `pub use` + one `scraper_entry!` line.
- **API router**: `src/api/router.rs::ROUTES` is a `&[(&str, ApiHandler)]` table. The `route!` macro adapts handlers across four arities (`(app)`, `(app, params)`, `(app, session, params)`, `(params)`).
- **Wikidata properties**: hard-coded P-numbers go in `src/util/wikidata_props.rs`. Don't sprinkle `"P31"` literals through the code; use `wp::P_INSTANCE_OF` etc.

## Conventions you'll see

- **Section-divider comments** (`// Catalog`, `// Issue`, `// Maintenance`) inside large traits / impl blocks. The audit pass uses them as natural sub-trait boundaries — preserve them when editing.
- **Long doc-comments on storage/maintenance methods** explaining *why* the SQL looks the way it does (PHP parity, MySQL optimiser hints, prior-incident workarounds). Read them before "simplifying".
- **`#[allow(clippy::cognitive_complexity)]` and `// #lizard forgives` markers** are usually a code smell — almost every one has been removed by the recent complexity refactors. If you find a remaining one, treat it as a TODO.
- **`Option<AppState>` self-injection in `Entry`** (the `set_app(&self.app)` pattern). It's a known SRP violation; flagged in the audit but currently load-bearing — don't refactor opportunistically.
- **`#[ignore = "requires database / external services — run with cargo test -- --ignored"]`** is the standard marker for live-DB tests. New live-DB tests should use the same string.

## Recent structural changes (so don't redo them)

- `storage_mysql.rs`, `auxiliary_matcher.rs`, `automatch.rs`, `maintenance.rs` are now **directories** with `mod.rs` + topical submodules.
- `Job::run_this_job`'s 305-line / 59-arm match is now three subsystem dispatchers.
- `api/router.rs::dispatch` is a static `ROUTES` table (was a 91-arm match).
- `bespoke_scrapers::run_bespoke_scraper` is a `SCRAPER_REGISTRY` slice (was a 23-arm match).
- A shared `reqwest::Client` lives on `AppState::http_client()`. Specialised clients (WDQS long timeout, OAuth flow, GND fetcher, custom user-agent scrapers) intentionally still build their own.
- `AppContext` trait exists in `app_state.rs`. Existing call sites still take `&AppState`; that's fine, they auto-satisfy the trait.

## Audits (gitignored, durable)

- `audits/code_complexity.md` — function/file size, branching, coupling. Most top findings now resolved.
- `audits/code_duplication.md` — code-duplication sweep.
- `audits/code_solid.md` — SOLID adherence. Documents what's been done and the remaining roadmap (full Storage segregation, Entry split, AppState decomposition).
- `audits/code_coverage.md` — `cargo llvm-cov` per-file coverage snapshot with totals, top performers, and zero-coverage gaps. Regenerate on demand (see below).

Read the audit before starting a large structural change — there's almost certainly already a recipe written for it.

### Regenerating `code_coverage.md`

`cargo-llvm-cov` is installed but needs the LLVM tools and a running Docker daemon (87 tests use a MariaDB testcontainer):

```bash
rustup component add llvm-tools-preview     # one-time
open -a Docker                              # macOS — wait until `docker ps` works
SYSROOT=$(rustc --print sysroot)
HOST=$(rustc -vV | sed -n 's/host: //p')
export LLVM_COV="$SYSROOT/lib/rustlib/$HOST/bin/llvm-cov"
export LLVM_PROFDATA="$SYSROOT/lib/rustlib/$HOST/bin/llvm-profdata"
cargo llvm-cov --lib --no-fail-fast > /tmp/cov.txt 2>&1
# table is the last section of /tmp/cov.txt; copy it into audits/code_coverage.md
```

`--no-fail-fast` keeps the report valid even when the two flaky network tests trip. Add `--summary-only` for just the totals, `--html` for a browsable report under `target/llvm-cov/html/`.

## Things to verify before claiming "done"

- `cargo build` clean.
- `cargo test --lib` all passing (812 expected; two flaky network tests excepted — re-run individually if they fail).
- For storage / SQL changes: at least one assertion-pinning unit test in the relevant test module.
- For new actions / endpoints / scrapers: the registry test in the same file (e.g. `route_table_contains_critical_endpoints`, `scraper_registry_contains_known_ids`) should still pass and ideally be extended.
