import { mnm_api, mnm_loading, mnm_notify, tt_update_interface, widar } from './store.js';

const TYPE_LABELS = {
	'MISMATCH': 'Value mismatch',
	'EXT_MISMATCH': 'External mismatch',
	'REMOVED_BEFORE': 'Property was removed before',
	'DATE_MISMATCH': 'Date mismatch',
	'OTHER_I': 'Other item already has data',
};

const STATUS_BADGE = {
	'OPEN':     'bg-warning text-dark',
	'DONE':     'bg-success',
	'REOPENED': 'bg-danger',
};

function getTypeLabel(type) { return TYPE_LABELS[type] || type; }
function getStatusBadge(status) { return STATUS_BADGE[status] || 'bg-secondary'; }

function prettyTime(ts) {
	if (!ts || ts.length < 14) return ts || '';
	return ts.substr(0, 4) + '-' + ts.substr(4, 2) + '-' + ts.substr(6, 2)
		+ ' ' + ts.substr(8, 2) + ':' + ts.substr(10, 2) + ':' + ts.substr(12, 2);
}

(function () {
	// Page-local styles. Kept inline so the file stays self-contained.
	const s = document.createElement('style');
	s.textContent = `
.mnm-lc-catalog-card { transition: box-shadow 0.15s; }
.mnm-lc-catalog-card:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
.mnm-lc-toolbar { display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center;
	padding:0.5rem; border:1px solid var(--mnm-border,#dee2e6); border-radius:0.25rem;
	background:var(--mnm-bg-alt,#f4f6f8); margin-bottom:0.75rem; }
.mnm-lc-filters .badge { font-size:0.75rem; }
.mnm-lc-filters .badge a { color:inherit; text-decoration:none; margin-left:0.4em; opacity:0.9; }
.mnm-lc-report-table { table-layout:auto; }
.mnm-lc-report-table th, .mnm-lc-report-table td { vertical-align:middle; }
.mnm-lc-report-table td.num, .mnm-lc-report-table th.num { text-align:right; min-width:4em; }
.mnm-lc-action-cell { min-width:5.5em; white-space:nowrap; }
`;
	document.head.appendChild(s);
})();

// ── Catalog list (top-level) ────────────────────────────────────────

var LcCatalogList = Vue.extend({
	data: function () { return { catalogs: [], open_issues: {}, loaded: false, error: '' }; },
	created: function () { this.load(); },
	updated: function () { tt_update_interface(); },
	methods: {
		load: async function () {
			var me = this;
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_catalogs');
				me.catalogs = d.data.catalogs || [];
				me.open_issues = d.data.open_issues || {};
			} catch (e) {
				me.error = e.message || 'Failed to load large catalogs';
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		issueCount: function (id) {
			var n = this.open_issues[id];
			return n ? (n * 1) : 0;
		},
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{text: 'Large catalogs'}]"></mnm-breadcrumb>
	<h1>Large catalogs</h1>
	<p class='text-muted'>External datasets too large to import into the standard Mix'n'match catalog table — managed in their own DB with periodic Wikidata syncs.</p>

	<div v-if='!loaded' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>⏳</div>
		<i tt='loading'></i>
	</div>
	<div v-else-if='error' class='alert alert-danger'>{{error}}</div>
	<div v-else>
		<div class='mb-3 d-flex flex-wrap gap-2 align-items-center'>
			<span class='small text-muted'>Recent changes:</span>
			<router-link to='/large_catalogs/rc' class='btn btn-outline-primary btn-sm'>All</router-link>
			<router-link to='/large_catalogs/rc?users=1' class='btn btn-outline-primary btn-sm'>Users only</router-link>
		</div>

		<h2 class='h5 mb-2'>Available catalogs</h2>
		<div v-if='catalogs.length==0' class='mnm-empty-state'>
			<div class='mnm-empty-icon'>∅</div>
			<i>No large catalogs configured.</i>
		</div>
		<div v-else class='row g-2'>
			<div v-for='c in catalogs' :key='c.id' class='col-12 col-md-6'>
				<div class='card mnm-lc-catalog-card h-100'>
					<div class='card-body py-2 px-3'>
						<div class='d-flex justify-content-between align-items-start gap-2'>
							<div class='flex-grow-1'>
								<router-link :to="'/large_catalogs/report/'+c.id"
									class='card-title h6 mb-1 d-block'>{{c.name}}</router-link>
								<small v-if='c.desc' class='text-muted d-block'>{{c.desc}}</small>
							</div>
							<div class='text-end'>
								<span v-if='issueCount(c.id)' class='badge bg-warning text-dark' :title="'Unresolved issues'">
									{{issueCount(c.id).toLocaleString()}}
								</span>
								<span v-else class='badge bg-success'>0</span>
								<div><small class='text-muted'>open issues</small></div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>`
});

// ── Report matrix ───────────────────────────────────────────────────

var LcReport = Vue.extend({
	props: ['catalog_id'],
	data: function () { return { catalog: null, matrix: [], loaded: false, error: '' }; },
	created: function () { this.load(); },
	updated: function () { tt_update_interface(); },
	computed: {
		grouped: function () {
			var by_prop = {};
			var all_statuses = {};
			this.matrix.forEach(function (r) {
				var p = 'P' + r.prop;
				if (!by_prop[p]) by_prop[p] = {};
				if (!by_prop[p][r.type]) by_prop[p][r.type] = { total: 0 };
				by_prop[p][r.type][r.status] = r.cnt * 1;
				by_prop[p][r.type].total += r.cnt * 1;
				all_statuses[r.status] = true;
			});
			// Stable status column ordering: OPEN, REOPENED, DONE, then any extras alphabetically.
			var preferred = ['OPEN', 'REOPENED', 'DONE'];
			var extras = Object.keys(all_statuses).filter(function (s) { return preferred.indexOf(s) === -1; }).sort();
			var statuses = preferred.filter(function (s) { return all_statuses[s]; }).concat(extras);
			return { by_prop: by_prop, statuses: statuses };
		},
		propKeys: function () { return Object.keys(this.grouped.by_prop).sort(); },
	},
	methods: {
		load: async function () {
			var me = this;
			me.loaded = false;
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_report', { catalog: me.catalog_id });
				me.catalog = d.data.catalog;
				me.matrix = d.data.matrix || [];
			} catch (e) {
				me.error = e.message || 'Failed to load report';
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		getTypeLabel: getTypeLabel,
		getStatusBadge: getStatusBadge,
		statusTitle: function (s) { return s.charAt(0) + s.slice(1).toLowerCase(); },
		types_for_prop: function (prop) {
			return Object.keys(this.grouped.by_prop[prop] || {});
		},
		linkFor: function (prop, type, status) {
			var q = 'type=' + type + '&prop=' + prop.replace(/^P/, '');
			if (status) q += '&status=' + status;
			return '/large_catalogs/report_list/' + this.catalog_id + '?' + q;
		},
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: 'Large catalogs', to: '/large_catalogs'},
		{text: catalog ? catalog.name : 'Report'}
	]"></mnm-breadcrumb>
	<h1 v-if='catalog' class='h3'>
		Reports on
		<router-link :to="'/large_catalogs/report_list/'+catalog_id">{{catalog.name}}</router-link>
	</h1>
	<p v-if='catalog && catalog.desc' class='text-muted'>{{catalog.desc}}</p>

	<div v-if='!loaded' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>⏳</div>
		<i tt='loading'></i>
	</div>
	<div v-else-if='error' class='alert alert-danger'>{{error}}</div>
	<div v-else-if='matrix.length==0' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>✓</div>
		<i>No reports for this catalog.</i>
	</div>
	<div v-else>
		<div v-for='prop in propKeys' :key='prop' class='mb-4'>
			<h2 class='h5'>
				<a class='external' :href="'https://www.wikidata.org/wiki/Property:'+prop" target='_blank' rel='noopener'>{{prop}}</a>
				<router-link :to="'/large_catalogs/report_list/'+catalog_id+'?prop='+prop.replace('P','')"
					class='btn btn-outline-secondary btn-sm ms-2'>View all</router-link>
			</h2>
			<div class='table-responsive'>
				<table class='table table-sm table-striped mnm-lc-report-table' style='width:auto'>
					<thead class='table-light'>
						<tr>
							<th>Type</th>
							<th v-for='s in grouped.statuses' :key='s' class='num'>
								<span class='badge' :class='getStatusBadge(s)'>{{statusTitle(s)}}</span>
							</th>
							<th class='num'>Total</th>
						</tr>
					</thead>
					<tbody>
						<tr v-for='type in types_for_prop(prop)' :key='type'>
							<th>{{getTypeLabel(type)}}</th>
							<td v-for='s in grouped.statuses' :key='s' class='num'>
								<router-link v-if='grouped.by_prop[prop][type] && grouped.by_prop[prop][type][s]'
									:to='linkFor(prop, type, s)'>{{grouped.by_prop[prop][type][s].toLocaleString()}}</router-link>
								<span v-else class='text-muted'>—</span>
							</td>
							<td class='num'>
								<router-link :to='linkFor(prop, type, "")'
									class='fw-bold'>{{grouped.by_prop[prop][type].total.toLocaleString()}}</router-link>
							</td>
						</tr>
					</tbody>
				</table>
			</div>
		</div>
	</div>
</div>`
});

// ── Shared row/filter helpers used by report-list and recent-changes ──

function rowActionButtons(rowVar) {
	// Returns a reusable template fragment for the per-row action cell.
	return `
<button v-if='`+rowVar+`.status!="DONE" && widar.is_logged_in' class='btn btn-outline-success btn-sm'
	@click.prevent='setStatus(`+rowVar+`,"DONE")' title='Mark as done'>Done</button>
<button v-else-if='`+rowVar+`.status=="DONE" && widar.is_logged_in' class='btn btn-outline-danger btn-sm'
	@click.prevent='setStatus(`+rowVar+`,"REOPENED")' title='Re-open'>Re-open</button>
<small v-else-if='!widar.is_logged_in' class='text-muted'>
	<a href='/widar/index.php?action=authorize' target='_blank' rel='noopener'>Log in</a>
</small>
`;
}

// ── Report list ─────────────────────────────────────────────────────

var LcReportList = Vue.extend({
	props: ['catalog_id'],
	data: function () { return { catalog: null, rows: [], total: 0, loaded: false, error: '',
		limit: 50, offset: 0, status: '', type: '', prop: '', user: '',
		page_size_options: [25, 50, 100, 200],
	}; },
	created: function () {
		var q = this.$route.query;
		if (q.status) this.status = q.status;
		if (q.type) this.type = q.type;
		if (q.prop) this.prop = ('' + q.prop).replace(/\D/g, '');
		if (q.user) this.user = q.user;
		if (q.offset) this.offset = q.offset * 1;
		if (q.limit) this.limit = q.limit * 1;
		this.load();
	},
	updated: function () { tt_update_interface(); },
	computed: {
		widar: function () { return widar; },
		hasFilters: function () { return !!(this.prop || this.status || this.type || this.user); },
	},
	methods: {
		load: async function () {
			var me = this;
			me.loaded = false;
			me.error = '';
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_report_list', {
					catalog: me.catalog_id, status: me.status, type: me.type,
					prop: me.prop, user: me.user, limit: me.limit, offset: me.offset
				});
				me.catalog = d.data.catalog;
				me.rows = d.data.rows || [];
				me.total = d.data.total || me.rows.length;
			} catch (e) {
				me.error = e.message || 'Failed to load report list';
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		buildQuery: function () {
			var q = {};
			if (this.status) q.status = this.status;
			if (this.type) q.type = this.type;
			if (this.prop) q.prop = this.prop;
			if (this.user) q.user = this.user;
			if (this.offset) q.offset = this.offset;
			if (this.limit != 50) q.limit = this.limit;
			return q;
		},
		pushQuery: function () {
			this.$router.push({
				path: '/large_catalogs/report_list/' + this.catalog_id,
				query: this.buildQuery(),
			});
		},
		goToPage: function (new_offset) {
			this.offset = new_offset;
			this.pushQuery();
			this.load();
			if (typeof window != 'undefined' && window.scrollTo) window.scrollTo(0, 0);
		},
		clearFilter: function (key) {
			this[key] = '';
			this.offset = 0;
			this.pushQuery();
			this.load();
		},
		changePageSize: function () {
			this.offset = 0;
			this.pushQuery();
			this.load();
		},
		setStatus: async function (row, new_status) {
			try {
				await mnm_api('lc_set_status', {
					id: row.id, status: new_status, user: widar.getUserName()
				});
				row.status = new_status;
			} catch (e) {
				mnm_notify(e.message || 'Failed', 'danger');
			}
		},
		getTypeLabel: getTypeLabel,
		getStatusBadge: getStatusBadge,
		prettyTime: prettyTime,
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: 'Large catalogs', to: '/large_catalogs'},
		{text: catalog ? catalog.name : '...', to: '/large_catalogs/report/'+catalog_id},
		{text: 'Report list'}
	]"></mnm-breadcrumb>
	<h1 v-if='catalog' class='h3'>
		<router-link :to="'/large_catalogs/report/'+catalog_id">{{catalog.name}}</router-link>
		<small class='text-muted'> · Report list</small>
	</h1>

	<div v-if='hasFilters' class='mnm-lc-toolbar mnm-lc-filters'>
		<span class='small text-muted me-2'>Filters:</span>
		<span v-if='prop' class='badge bg-info text-dark'>P{{prop}} <a href='#' @click.prevent='clearFilter("prop")' title='Clear'>×</a></span>
		<span v-if='status' class='badge' :class='getStatusBadge(status)'>{{status}} <a href='#' @click.prevent='clearFilter("status")' title='Clear'>×</a></span>
		<span v-if='type' class='badge bg-secondary'>{{getTypeLabel(type)}} <a href='#' @click.prevent='clearFilter("type")' title='Clear'>×</a></span>
		<span v-if='user' class='badge bg-secondary'>User: {{user}} <a href='#' @click.prevent='clearFilter("user")' title='Clear'>×</a></span>
		<span class='ms-auto small text-muted' v-if='loaded'>{{total.toLocaleString()}} matching reports</span>
	</div>

	<div v-if='!loaded' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>⏳</div>
		<i tt='loading'></i>
	</div>
	<div v-else-if='error' class='alert alert-danger'>{{error}}</div>
	<div v-else-if='rows.length==0' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>✓</div>
		<i>No results.</i>
	</div>
	<div v-else>
		<div class='d-flex justify-content-between align-items-center mb-2'>
			<small class='text-muted' v-if='!hasFilters'>{{total.toLocaleString()}} reports</small>
			<select v-model.number='limit' @change='changePageSize' class='form-select form-select-sm' style='width:auto'>
				<option v-for='n in page_size_options' :value='n'>{{n}}/page</option>
			</select>
		</div>

		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>

		<div class='table-responsive'>
			<table class='table table-sm table-striped table-hover align-middle'>
				<thead class='table-light'>
					<tr>
						<th>#</th>
						<th>Item</th>
						<th v-if='!prop'>Property</th>
						<th>Message</th>
						<th v-if='!status'>Status</th>
						<th v-if='!type'>Type</th>
						<th v-if='!user'>User</th>
						<th>Time</th>
						<th>Action</th>
					</tr>
				</thead>
				<tbody>
					<tr v-for='(row, idx) in rows' :key='row.id'>
						<td><small class='text-muted'>{{(offset + idx + 1).toLocaleString()}}</small></td>
						<td><a class='external' :href="'https://www.wikidata.org/wiki/Q'+row.q" target='_blank' rel='noopener'>Q{{row.q}}</a></td>
						<td v-if='!prop'>
							<a class='external' :href="'https://www.wikidata.org/wiki/Property:P'+row.prop" target='_blank' rel='noopener'>P{{row.prop}}</a>
						</td>
						<td><small v-html='row.html || row.message'></small></td>
						<td v-if='!status'>
							<span class='badge' :class='getStatusBadge(row.status)'>{{row.status}}</span>
						</td>
						<td v-if='!type'><small>{{getTypeLabel(row.type)}}</small></td>
						<td v-if='!user' nowrap>
							<small v-if='row.user'>
								<a class='external' :href="'https://www.wikidata.org/wiki/User:'+encodeURIComponent(row.user)" target='_blank' rel='noopener'>{{row.user}}</a>
							</small>
						</td>
						<td nowrap><small>{{prettyTime(row.timestamp)}}</small></td>
						<td class='mnm-lc-action-cell'>
							<button v-if='row.status!="DONE" && widar.is_logged_in' class='btn btn-outline-success btn-sm'
								@click.prevent='setStatus(row,"DONE")' title='Mark as done'>Done</button>
							<button v-else-if='row.status=="DONE" && widar.is_logged_in' class='btn btn-outline-danger btn-sm'
								@click.prevent='setStatus(row,"REOPENED")' title='Re-open'>Re-open</button>
							<small v-else-if='!widar.is_logged_in' class='text-muted'>
								<a href='/widar/index.php?action=authorize' target='_blank' rel='noopener'>Log in</a>
							</small>
						</td>
					</tr>
				</tbody>
			</table>
		</div>

		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			@go-to-page='goToPage'></pagination>
	</div>
</div>`
});

// ── Recent changes ──────────────────────────────────────────────────

var LcRecentChanges = Vue.extend({
	data: function () { return { rows: [], total: 0, loaded: false, error: '',
		limit: 50, offset: 0, users: 0, page_size_options: [25, 50, 100, 200],
	}; },
	created: function () {
		if (this.$route.query.users) this.users = this.$route.query.users * 1;
		if (this.$route.query.offset) this.offset = this.$route.query.offset * 1;
		if (this.$route.query.limit) this.limit = this.$route.query.limit * 1;
		this.load();
	},
	updated: function () { tt_update_interface(); },
	computed: {
		widar: function () { return widar; },
	},
	methods: {
		load: async function () {
			var me = this;
			me.loaded = false;
			me.error = '';
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_rc', { limit: me.limit, offset: me.offset, users: me.users });
				me.rows = d.data.rows || [];
				me.total = d.data.total || me.rows.length;
			} catch (e) {
				me.error = e.message || 'Failed to load recent changes';
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		pushQuery: function () {
			var q = {};
			if (this.users) q.users = 1;
			if (this.offset) q.offset = this.offset;
			if (this.limit != 50) q.limit = this.limit;
			this.$router.push({ path: '/large_catalogs/rc', query: q });
		},
		goToPage: function (new_offset) {
			this.offset = new_offset;
			this.pushQuery();
			this.load();
			if (typeof window != 'undefined' && window.scrollTo) window.scrollTo(0, 0);
		},
		toggleUsersOnly: function () {
			this.users = this.users ? 0 : 1;
			this.offset = 0;
			this.pushQuery();
			this.load();
		},
		changePageSize: function () {
			this.offset = 0;
			this.pushQuery();
			this.load();
		},
		setStatus: async function (row, new_status) {
			try {
				await mnm_api('lc_set_status', {
					id: row.id, status: new_status, user: widar.getUserName()
				});
				row.status = new_status;
			} catch (e) {
				mnm_notify(e.message || 'Failed', 'danger');
			}
		},
		getTypeLabel: getTypeLabel,
		getStatusBadge: getStatusBadge,
		prettyTime: prettyTime,
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: 'Large catalogs', to: '/large_catalogs'},
		{text: users ? 'Recent changes (users)' : 'Recent changes'}
	]"></mnm-breadcrumb>
	<h1 class='h3'>Recent changes <small v-if='users' class='text-muted'>(users only)</small></h1>

	<div class='mnm-lc-toolbar'>
		<button class='btn btn-outline-secondary btn-sm' @click.prevent='toggleUsersOnly'>
			{{ users ? 'Show all' : 'Show users only' }}
		</button>
		<select v-model.number='limit' @change='changePageSize' class='form-select form-select-sm' style='width:auto'>
			<option v-for='n in page_size_options' :value='n'>{{n}}/page</option>
		</select>
		<span class='ms-auto small text-muted' v-if='loaded'>{{total.toLocaleString()}} entries</span>
	</div>

	<div v-if='!loaded' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>⏳</div>
		<i tt='loading'></i>
	</div>
	<div v-else-if='error' class='alert alert-danger'>{{error}}</div>
	<div v-else-if='rows.length==0' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>∅</div>
		<i>No results.</i>
	</div>
	<div v-else>
		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>

		<div class='table-responsive'>
			<table class='table table-sm table-striped table-hover align-middle'>
				<thead class='table-light'>
					<tr>
						<th>#</th><th>Item</th><th>Property</th><th>Message</th>
						<th>Status</th><th>Type</th><th>User</th><th>Time</th><th>Action</th>
					</tr>
				</thead>
				<tbody>
					<tr v-for='(row, idx) in rows' :key='row.id'>
						<td><small class='text-muted'>{{(offset + idx + 1).toLocaleString()}}</small></td>
						<td><a class='external' :href="'https://www.wikidata.org/wiki/Q'+row.q" target='_blank' rel='noopener'>Q{{row.q}}</a></td>
						<td><a class='external' :href="'https://www.wikidata.org/wiki/Property:P'+row.prop" target='_blank' rel='noopener'>P{{row.prop}}</a></td>
						<td><small v-html='row.html || row.message'></small></td>
						<td><span class='badge' :class='getStatusBadge(row.status)'>{{row.status}}</span></td>
						<td><small>{{getTypeLabel(row.type)}}</small></td>
						<td nowrap>
							<small v-if='row.user'>
								<a class='external' :href="'https://www.wikidata.org/wiki/User:'+encodeURIComponent(row.user)" target='_blank' rel='noopener'>{{row.user}}</a>
							</small>
						</td>
						<td nowrap><small>{{prettyTime(row.timestamp)}}</small></td>
						<td class='mnm-lc-action-cell'>
							<button v-if='row.status!="DONE" && widar.is_logged_in' class='btn btn-outline-success btn-sm'
								@click.prevent='setStatus(row,"DONE")' title='Mark as done'>Done</button>
							<button v-else-if='row.status=="DONE" && widar.is_logged_in' class='btn btn-outline-danger btn-sm'
								@click.prevent='setStatus(row,"REOPENED")' title='Re-open'>Re-open</button>
							<small v-else-if='!widar.is_logged_in' class='text-muted'>
								<a href='/widar/index.php?action=authorize' target='_blank' rel='noopener'>Log in</a>
							</small>
						</td>
					</tr>
				</tbody>
			</table>
		</div>

		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			@go-to-page='goToPage'></pagination>
	</div>
</div>`
});

// ── Exports ─────────────────────────────────────────────────────────

export { LcCatalogList, LcReport, LcReportList, LcRecentChanges };
export default LcCatalogList;
