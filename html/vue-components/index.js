/**
 * MnM Vue component barrel — registers global sub-components and exports
 * page components for VueRouter (with lazy loading for infrequent pages).
 *
 * Import this module via dynamic import() from index_vue.js:
 *
 *   import('./vue-components/index.js').then(mnm => {
 *     // mnm.MainPage, mnm.CatalogDetails, etc.
 *   });
 *
 * Frequently visited pages (main, catalog, list, entry, search, rc) are
 * imported eagerly so they render without a second network round-trip.
 * Infrequent pages (import wizard, scraper, map, sync, etc.) use
 * () => import() for on-demand loading — they are only fetched when the
 * user navigates to that route.
 */

// ── 1. Register magnustools external components ──────────────────────────────
import { registerAll } from '/resources/vue_es6/index.js';
registerAll(Vue);

// ── 2. Register MnM global sub-components (needed in templates everywhere) ───

import Timestamp from './timestamp.js';
Vue.component('timestamp', Timestamp);

import EntryLink from './entry_link.js';
Vue.component('entry-link', EntryLink);

import CatalogHeader from './catalog_header.js';
Vue.component('catalog-header', CatalogHeader);

import MnmBreadcrumb from './mnm_breadcrumb.js';
Vue.component('mnm-breadcrumb', MnmBreadcrumb);

import CatalogSlice from './catalog_slice.js';
Vue.component('catalog-slice', CatalogSlice);

import CatalogPreview from './catalog_preview.js';
Vue.component('catalog-preview', CatalogPreview);

import CatalogSearchPicker from './catalog_search_picker.js';
Vue.component('catalog-search-picker', CatalogSearchPicker);

import CatalogListItem from './catalog_list_item.js';
Vue.component('catalog-list-item', CatalogListItem);

import Pagination from '../resources/vue_es6/pagination.js';
Vue.component('pagination', Pagination);

import RcEventsList from './rc_events_list.js';
Vue.component('rc-events-list', RcEventsList);

import CatalogActionsDropdown from './catalog_actions_dropdown.js';
Vue.component('catalog-actions-dropdown', CatalogActionsDropdown);

import EntryListItem from './entry_list_item.js';
Vue.component('entry-list-item', EntryListItem);

import EntryDetails from './entry_details.js';
Vue.component('entry-details', EntryDetails);

import CatalogEntryMultiMatch from './catalog_entry_multi_match.js';
Vue.component('catalog-entry-multi-match', CatalogEntryMultiMatch);

import CodeComponent from './code.js';
Vue.component('code-fragment', CodeComponent);

import MatchEntry from './match_entry.js';
Vue.component('match-entry', MatchEntry);

import Translator from './translator.js';
Vue.component('translator', Translator);

import WidarComp, { UserLink } from './widar.js';
Vue.component('widar', WidarComp);
Vue.component('userlink', UserLink);

import WikidataMap from './wikidatamap.js';
Vue.component('wikidata-map', WikidataMap);

import SearchPageComp, { SearchBox } from './search_page.js';
Vue.component('search-box', SearchBox);

// ── 3. Eager page imports (hot paths: main, catalog, list, entry, search, rc) ─

import MainPage from './main_page.js';
import CatalogGroup from './catalog_group.js';
import CatalogDetails from './catalog_details.js';
import CatalogList from './catalog_list.js';
import Entry from './entry_page.js';
import RecentChanges from './recent_changes.js';
import RandomEntry from './random_entry.js';
import CreationCandidates from './creation_candidates.js';

// Register components that are also used as sub-components in other templates
Vue.component('catalog-list', CatalogList);
Vue.component('catalog-details', CatalogDetails);

// ── 4. Lazy page imports (infrequent routes — loaded on first navigation) ────
//
// Vue Router accepts a function returning a Promise for the `component` field.
// These dynamic imports are only fetched when the route is first visited.
// Each returns { default: ComponentDef }, so we unwrap with .then(m => m.default).

function lazy(loader) {
	return function () { return loader().then(function (m) { return m.default || m; }); };
}

var CatalogEditor     = lazy(function () { return import('./catalog_editor.js'); });
var Aliases           = lazy(function () { return import('./aliases.js'); });
var Jobs              = lazy(function () { return import('./jobs.js'); });
var Import            = lazy(function () { return import('./import.js'); });
var Issues            = lazy(function () { return import('./issues.js'); });
var CommonNames       = lazy(function () { return import('./common_names.js'); });
var ByPropertyValue   = lazy(function () { return import('./by_property_value.js'); });
var PeoplePage        = lazy(function () { return import('./people_page.js'); });
var EntriesPage       = lazy(function () { return import('./entries_page.js'); });
var QuickComparePage  = lazy(function () { return import('./quick_compare_page.js'); });
var StatementTextPage = lazy(function () { return import('./statement_text.js'); });
var DownloadPage      = lazy(function () { return import('./download_page.js'); });
var SyncCatalog       = lazy(function () { return import('./sync_catalog.js'); });
var Scraper           = lazy(function () { return import('./scraper.js'); });
var SparqlList        = lazy(function () { return import('./sparql_list.js'); });
var TopMissingEntries = lazy(function () { return import('./top_missing_entries.js'); });
var CodePage          = lazy(function () { return import('./codepage.js'); });
var TopGroups         = lazy(function () { return import('./top_groups.js'); });
var MapPage           = lazy(function () { return import('./map_page.js'); });
var MobileMatch       = lazy(function () { return import('./mobile_match.js'); });
var MissingPropertiesPage = lazy(function () { return import('./missing_properties.js'); });
var MnmTargetsPage    = lazy(function () { return import('./mnm_targets.js'); });
var UserEdits         = lazy(function () { return import('./user_edits.js'); });
var VisualMatch       = lazy(function () { return import('./visual_match.js'); });

// missing_articles.js exports two components; handle the named export
var SiteStats = lazy(function () { return import('./missing_articles.js'); });
var MissingArticles = function () {
	return import('./missing_articles.js').then(function (m) { return m.MissingArticles; });
};

// large_catalogs.js exports multiple sub-page components
var LcCatalogList = lazy(function () { return import('./large_catalogs.js'); });
var LcReport = function () {
	return import('./large_catalogs.js').then(function (m) { return m.LcReport; });
};
var LcReportList = function () {
	return import('./large_catalogs.js').then(function (m) { return m.LcReportList; });
};
var LcRecentChanges = function () {
	return import('./large_catalogs.js').then(function (m) { return m.LcRecentChanges; });
};

// ── 5. Export all page components for VueRouter ──────────────────────────────

export {
	MainPage,
	CatalogGroup,
	CatalogDetails,
	CatalogList,
	Entry,
	RecentChanges,
	CreationCandidates,
	CatalogEditor,
	Aliases,
	Jobs,
	Import,
	Issues,
	CommonNames,
	ByPropertyValue,
	PeoplePage,
	EntriesPage,
	QuickComparePage,
	StatementTextPage,
	DownloadPage,
	SiteStats,
	MissingArticles,
	RandomEntry,
	SyncCatalog,
	Scraper,
	SparqlList,
	TopMissingEntries,
	CodePage,
	TopGroups,
	MapPage,
	MobileMatch,
	MissingPropertiesPage,
	MnmTargetsPage,
	UserEdits,
	LcCatalogList,
	LcReport,
	LcReportList,
	LcRecentChanges,
	VisualMatch,
	SearchPageComp as SearchPage,
};
