// ENFORCE HTTPS
if (location.protocol != 'https:') location.href = 'https:' + window.location.href.substring(window.location.protocol.length);

// ==================== Global error handler ====================

Vue.config.errorHandler = function (err, vm, info) {
	console.error('[Vue error]', info, err);
	window.mnm_notify('Something went wrong: ' + (err.message || err), 'danger', 8000);
};

window.addEventListener('unhandledrejection', function (event) {
	console.error('[Unhandled promise rejection]', event.reason);
	window.mnm_notify('Request failed: ' + (event.reason && event.reason.message || event.reason || 'unknown error'), 'danger', 6000);
});

// ==================== Boot ========================================
// The store module (store.js) is the source of truth for all shared
// state and utilities. We expose thin window.* aliases here so that
// (a) the error handler above can call mnm_notify before modules load,
// (b) external magnustools components that read globals still work.

var wd, widar, router, app, tt;

// Stub mnm_notify until the real one loads from store.js
window.mnm_notify = function () {};

document.addEventListener('DOMContentLoaded', function () {
	import('./vue-components/store.js').then(function (store) {
		// The real mnm_notify — replaces the stub above so the error
		// handler works even before components load.
		window.mnm_notify = store.mnm_notify;

		wd = new WikiData();
		store.setWd(wd);
		tt = new ToolTranslation({
			tool: 'mix-n-match',
			fallback: 'en',
			highlight_missing: true,
		});
		store.setTt(tt);

		return import('./vue-components/index.js');
	}).then(function (mnm) {
		var routes = [
			{ path: '/', component: mnm.MainPage },
			{ path: '/group', component: mnm.CatalogGroup, props: true },
			{ path: '/group/:key', component: mnm.CatalogGroup, props: true },
			{ path: '/group/:key/:order', component: mnm.CatalogGroup, props: true },
			{ path: '/main', component: mnm.MainPage },
			{ path: '/catalog/:id', component: mnm.CatalogDetails, props: true },
			{ path: '/list/:id/:mode', component: mnm.CatalogList, props: true },
			{ path: '/list/:id/:mode/:start', component: mnm.CatalogList, props: true },
			{ path: '/search', component: mnm.SearchPage, props: true },
			{ path: '/search/:query', component: mnm.SearchPage, props: true },
			{ path: '/search/:query/:excl', component: mnm.SearchPage, props: true },
			{ path: '/entry/:id', component: mnm.Entry, props: true },
			{ path: '/.2Fentry.2F:id', redirect: '/entry/:id' },
			{ path: '/rc', component: mnm.RecentChanges, props: true },
			{ path: '/rc/:catalog', component: mnm.RecentChanges, props: true },
			{ path: '/creation_candidates', component: mnm.CreationCandidates, props: true },
			{ path: '/creation_candidates/:mode', component: mnm.CreationCandidates, props: true },
			{ path: '/catalog_editor/:id', component: mnm.CatalogEditor, props: true },
			{ path: '/aliases/:id', component: mnm.Aliases, props: true },
			{ path: '/jobs', component: mnm.Jobs, props: true },
			{ path: '/import', component: mnm.Import, props: true },
			{ path: '/import/:original_catalog_id', component: mnm.Import, props: true },
			{ path: '/jobs/:id', component: mnm.Jobs, props: true },
			{ path: '/issues', component: mnm.Issues, props: true },
			{ path: '/issues/:type', component: mnm.Issues, props: true },
			{ path: '/issues/:type/:initial_catalogs', component: mnm.Issues, props: true },
			{ path: '/common_names/:id', component: mnm.CommonNames, props: true },
			{ path: '/by_property_value/:property/:value', component: mnm.ByPropertyValue, props: true },
			{ path: '/people/', component: mnm.PeoplePage, props: true },
			{ path: '/people/:gender', component: mnm.PeoplePage, props: true },
			{ path: '/entries/', component: mnm.EntriesPage, props: true },
			{ path: '/quick_compare', component: mnm.QuickComparePage, props: true },
			{ path: '/quick_compare/:catalog_id', component: mnm.QuickComparePage, props: true },
			{ path: '/statement_text/:id', component: mnm.StatementTextPage, props: true },
			{ path: '/download', component: mnm.DownloadPage, props: true },
			{ path: '/download/:catalogs', component: mnm.DownloadPage, props: true },
			{ path: '/site_stats/:id', component: mnm.SiteStats, props: true },
			{ path: '/missing_articles/:id/:site', component: mnm.MissingArticles, props: true },
			{ path: '/missing_articles/:id/:site/:start', component: mnm.MissingArticles, props: true },
			{ path: '/random', component: mnm.RandomEntry, props: true },
			{ path: '/random/:id', component: mnm.RandomEntry, props: true },
			{ path: '/random/:id/:mode', component: mnm.RandomEntry, props: true },
			{ path: '/sync/:id', component: mnm.SyncCatalog, props: true },
			{ path: '/scraper/new', component: mnm.Scraper, props: true },
			{ path: '/sparql', component: mnm.SparqlList, props: true },
			{ path: '/sparql/:sparql', component: mnm.SparqlList, props: true },
			{ path: '/top_missing/', component: mnm.TopMissingEntries, props: true },
			{ path: '/top_missing/:catalogs', component: mnm.TopMissingEntries, props: true },
			{ path: '/code/:catalog_id', component: mnm.CodePage, props: true },
			{ path: '/top_groups', component: mnm.TopGroups, props: true },
			{ path: '/top_groups/:id', component: mnm.TopGroups, props: true },
			{ path: '/map/:id', component: mnm.MapPage, props: true },
			{ path: '/map/:id/:entry_id', component: mnm.MapPage, props: true },
			{ path: '/mobile_match', component: mnm.MobileMatch, props: true },
			{ path: '/mobile_match/:id', component: mnm.MobileMatch, props: true },
			{ path: '/missing_properties', component: mnm.MissingPropertiesPage, props: true },
			{ path: '/mnm_targets', component: mnm.MnmTargetsPage, props: true },
			{ path: '/mnm_targets/:property', component: mnm.MnmTargetsPage, props: true },
			{ path: '/user/:user_id', component: mnm.UserEdits, props: true },
			{ path: '/user/:user_id/:catalog_id', component: mnm.UserEdits, props: true },
			{ path: '/large_catalogs', component: mnm.LcCatalogList, props: true },
			{ path: '/large_catalogs/report/:catalog_id', component: mnm.LcReport, props: true },
			{ path: '/large_catalogs/report_list/:catalog_id', component: mnm.LcReportList, props: true },
			{ path: '/large_catalogs/rc', component: mnm.LcRecentChanges, props: true },
			{ path: '/visual_match', component: mnm.VisualMatch, props: true },
			{ path: '/visual_match/entry/:id', component: mnm.VisualMatch, props: true },
			{ path: '/visual_match/:catalog_param', component: mnm.VisualMatch, props: true },
		];

		router = new VueRouter({ routes });
		function updateTitle(to) {
			var title = 'Mix\'n\'match';
			var p = to.path;
			if (p.match(/^\/catalog\//)) title += ' \u2013 Catalog ' + to.params.id;
			else if (p.match(/^\/search\//)) title += ' \u2013 Search: ' + decodeURIComponent(to.params.query || '');
			else if (p.match(/^\/list\//)) title += ' \u2013 Catalog ' + to.params.id + ' list';
			else if (p.match(/^\/entry\//)) title += ' \u2013 Entry ' + to.params.id;
			else if (p.match(/^\/jobs\//)) title += ' \u2013 Jobs for catalog ' + to.params.id;
			else if (p.match(/^\/jobs/)) title += ' \u2013 Jobs';
			else if (p.match(/^\/rc/)) title += ' \u2013 Recent Changes';
			else if (p.match(/^\/random/)) title += ' \u2013 Random match';
			else if (p.match(/^\/mobile_match/)) title += ' \u2013 Quick match';
			else if (p.match(/^\/creation_candidates/)) title += ' \u2013 Creation candidates';
			else if (p.match(/^\/sync\//)) title += ' \u2013 Sync catalog ' + to.params.id;
			else if (p.match(/^\/catalog_editor\//)) title += ' \u2013 Catalog editor ' + to.params.id;
			else if (p.match(/^\/group/)) title += ' \u2013 Catalog group';
			else if (p.match(/^\/import/)) title += ' \u2013 Import';
			else if (p.match(/^\/issues/)) title += ' \u2013 Issues';
			else if (p.match(/^\/download/)) title += ' \u2013 Download';
			else if (p.match(/^\/user\//)) title += ' \u2013 User edits';
			document.title = title;
		}
		router.afterEach(updateTitle);
		app = new Vue({ router }).$mount('#app');
		updateTitle(router.currentRoute);
		tt.addILdropdown('#tooltranslate_wrapper');
		tt.updateInterface(document.body);
	});
});
