import { mnm_api, mnm_loading, mnm_notify, tt_update_interface, widar } from './store.js';

var types_label = {
	'MISMATCH': 'Value mismatch',
	'EXT_MISMATCH': 'External mismatch',
	'REMOVED_BEFORE': 'Property was removed before',
	'DATE_MISMATCH': 'Date mismatch',
	'OTHER_I': 'Other item already has data'
};

function getTypeLabel(type) {
	return types_label[type] || type;
}

function prettyTime(ts) {
	if (!ts || ts.length < 14) return ts || '';
	return ts.substr(0, 4) + '-' + ts.substr(4, 2) + '-' + ts.substr(6, 2) + ' ' + ts.substr(8, 2) + ':' + ts.substr(10, 2) + ':' + ts.substr(12, 2);
}

// ── Catalog list (top-level) ────────────────────────────────────────

var LcCatalogList = Vue.extend({
	data: function () { return { catalogs: [], open_issues: {}, loaded: false }; },
	created: function () { this.load(); },
	updated: function () { tt_update_interface(); },
	methods: {
		load: async function () {
			var me = this;
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_catalogs');
				me.catalogs = d.data.catalogs;
				me.open_issues = d.data.open_issues;
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		}
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[{text: 'Large catalogs'}]"></mnm-breadcrumb>
	<h2>Large catalogs</h2>
	<div v-if='!loaded' class='text-center py-4'><i tt='loading'></i></div>
	<div v-else>
		<p>
			Recent changes:
			<router-link to="/large_catalogs/rc">all</router-link>,
			<router-link to="/large_catalogs/rc?users=1">users only</router-link>
		</p>
		<h3>Available catalogs</h3>
		<ul>
			<li v-for='c in catalogs' :key='c.id'>
				<b>{{c.name}}</b>
				(<router-link :to="'/large_catalogs/report/'+c.id">reports</router-link>)
				<span v-if='open_issues[c.id]'>, {{open_issues[c.id].toLocaleString()}} unresolved issues</span>
			</li>
		</ul>
	</div>
</div>`
});

// ── Report matrix ───────────────────────────────────────────────────

var LcReport = Vue.extend({
	props: ['catalog_id'],
	data: function () { return { catalog: null, matrix: [], loaded: false }; },
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
			return { by_prop: by_prop, statuses: Object.keys(all_statuses) };
		}
	},
	methods: {
		load: async function () {
			var me = this;
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_report', { catalog: me.catalog_id });
				me.catalog = d.data.catalog;
				me.matrix = d.data.matrix;
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		getTypeLabel: getTypeLabel,
		types_for_prop: function (prop) {
			return Object.keys(this.grouped.by_prop[prop] || {});
		}
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: 'Large catalogs', to: '/large_catalogs'},
		{text: catalog ? catalog.name : 'Report'}
	]"></mnm-breadcrumb>
	<h2 v-if='catalog'>Reports on <router-link :to="'/large_catalogs/report_list/'+catalog_id">{{catalog.name}}</router-link> dataset</h2>
	<div v-if='!loaded' class='text-center py-4'><i tt='loading'></i></div>
	<div v-else-if='matrix.length==0' class='text-muted py-3'>No reports for this catalog.</div>
	<div v-else>
		<div v-for='(data, prop) in grouped.by_prop' :key='prop' class='mb-4'>
			<h4><a class='external' :href="'https://www.wikidata.org/wiki/Property:'+prop" target='_blank'>{{prop}}</a></h4>
			<table class='table table-sm table-striped' style='width:auto'>
				<thead><tr>
					<th>Type</th>
					<th v-for='s in grouped.statuses' :key='s'>{{s.charAt(0)+s.slice(1).toLowerCase()}}</th>
					<th>Total</th>
				</tr></thead>
				<tbody>
					<tr v-for='type in types_for_prop(prop)' :key='type'>
						<th>{{getTypeLabel(type)}}</th>
						<td v-for='s in grouped.statuses' :key='s'>
							<router-link v-if='data[type] && data[type][s]'
								:to="'/large_catalogs/report_list/'+catalog_id+'?status='+s+'&type='+type+'&prop='+prop">{{data[type][s]}}</router-link>
						</td>
						<td>
							<router-link :to="'/large_catalogs/report_list/'+catalog_id+'?type='+type+'&prop='+prop">{{data[type].total}}</router-link>
						</td>
					</tr>
				</tbody>
			</table>
		</div>
	</div>
</div>`
});

// ── Report list ─────────────────────────────────────────────────────

var LcReportList = Vue.extend({
	props: ['catalog_id'],
	data: function () { return { catalog: null, rows: [], total: 0, loaded: false, limit: 20, offset: 0, status: '', type: '', prop: '', user: '' }; },
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
	methods: {
		load: async function () {
			var me = this;
			me.loaded = false;
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_report_list', {
					catalog: me.catalog_id, status: me.status, type: me.type,
					prop: me.prop, user: me.user, limit: me.limit, offset: me.offset
				});
				me.catalog = d.data.catalog;
				me.rows = d.data.rows;
				me.total = d.data.total || 0;
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		goToPage: function (new_offset) {
			var me = this;
			me.offset = new_offset;
			var query = {};
			if (me.status) query.status = me.status;
			if (me.type) query.type = me.type;
			if (me.prop) query.prop = me.prop;
			if (me.user) query.user = me.user;
			if (me.offset) query.offset = me.offset;
			if (me.limit != 20) query.limit = me.limit;
			me.$router.push({ path: '/large_catalogs/report_list/' + me.catalog_id, query: query });
			me.load();
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
		prettyTime: prettyTime
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: 'Large catalogs', to: '/large_catalogs'},
		{text: catalog ? catalog.name : '...', to: '/large_catalogs/report/'+catalog_id},
		{text: 'Report list'}
	]"></mnm-breadcrumb>
	<h2 v-if='catalog'>Report list: <router-link :to="'/large_catalogs/report/'+catalog_id">{{catalog.name}}</router-link></h2>

	<div class='d-flex flex-wrap gap-2 mb-2 small text-muted'>
		<span v-if='prop'>Property: <b>P{{prop}}</b></span>
		<span v-if='status'>Status: <b>{{status}}</b></span>
		<span v-if='type'>Type: <b>{{getTypeLabel(type)}}</b></span>
		<span v-if='user'>User: <b>{{user}}</b></span>
	</div>

	<div v-if='!loaded' class='text-center py-4'><i tt='loading'></i></div>
	<div v-else-if='rows.length==0' class='text-muted py-3'>No results.</div>
	<div v-else>
		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>
		<table class='table table-sm table-striped'>
			<thead><tr>
				<th>#</th>
				<th>Item</th>
				<th v-if='!prop'>Property</th>
				<th>Message</th>
				<th v-if='!status'>Status</th>
				<th v-if='!type'>Type</th>
				<th v-if='!user'>User</th>
				<th>Time</th>
				<th>Action</th>
			</tr></thead>
			<tbody>
				<tr v-for='(row, idx) in rows' :key='row.id'>
					<td>{{offset + idx + 1}}</td>
					<td><a class='external' :href="'https://www.wikidata.org/wiki/Q'+row.q" target='_blank'>Q{{row.q}}</a></td>
					<td v-if='!prop'><a class='external' :href="'https://www.wikidata.org/wiki/Property:P'+row.prop" target='_blank'>P{{row.prop}}</a></td>
					<td><small v-html='row.html || row.message'></small></td>
					<td v-if='!status'>
						<span :style="row.status=='REOPENED' ? 'color:red' : ''">{{row.status}}</span>
					</td>
					<td v-if='!type'>{{getTypeLabel(row.type)}}</td>
					<td v-if='!user' nowrap>
						<small v-if='row.user'><a class='external' :href="'https://www.wikidata.org/wiki/User:'+encodeURIComponent(row.user)" target='_blank'>{{row.user}}</a></small>
					</td>
					<td nowrap><small>{{prettyTime(row.timestamp)}}</small></td>
					<td>
						<button v-if='row.status!="DONE" && widar.is_logged_in' class='btn btn-dark btn-sm' @click.prevent='setStatus(row,"DONE")'>Done</button>
						<button v-else-if='row.status=="DONE" && widar.is_logged_in' class='btn btn-danger btn-sm' @click.prevent='setStatus(row,"REOPENED")'>Re-open</button>
						<small v-else-if='!widar.is_logged_in'><a href='/widar/index.php?action=authorize' target='_blank'>Log in</a></small>
					</td>
				</tr>
			</tbody>
		</table>
		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			@go-to-page='goToPage'></pagination>
	</div>
</div>`
});

// ── Recent changes ──────────────────────────────────────────────────

var LcRecentChanges = Vue.extend({
	data: function () { return { rows: [], total: 0, loaded: false, limit: 50, offset: 0, users: 0 }; },
	created: function () {
		if (this.$route.query.users) this.users = this.$route.query.users * 1;
		if (this.$route.query.offset) this.offset = this.$route.query.offset * 1;
		this.load();
	},
	updated: function () { tt_update_interface(); },
	methods: {
		load: async function () {
			var me = this;
			me.loaded = false;
			mnm_loading(true);
			try {
				var d = await mnm_api('lc_rc', { limit: me.limit, offset: me.offset, users: me.users });
				me.rows = d.data.rows;
				me.total = d.data.total || 0;
			} finally {
				me.loaded = true;
				mnm_loading(false);
			}
		},
		goToPage: function (new_offset) {
			var me = this;
			me.offset = new_offset;
			var query = {};
			if (me.users) query.users = 1;
			if (me.offset) query.offset = me.offset;
			me.$router.push({ path: '/large_catalogs/rc', query: query });
			me.load();
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
		prettyTime: prettyTime
	},
	template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: 'Large catalogs', to: '/large_catalogs'},
		{text: users ? 'Recent changes (users)' : 'Recent changes'}
	]"></mnm-breadcrumb>
	<h2>Recent changes <small v-if='users'>(users only)</small></h2>
	<div v-if='!loaded' class='text-center py-4'><i tt='loading'></i></div>
	<div v-else-if='rows.length==0' class='text-muted py-3'>No results.</div>
	<div v-else>
		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>
		<table class='table table-sm table-striped'>
			<thead><tr>
				<th>#</th><th>Item</th><th>Property</th><th>Message</th><th>Status</th><th>Type</th><th>User</th><th>Time</th><th>Action</th>
			</tr></thead>
			<tbody>
				<tr v-for='(row, idx) in rows' :key='row.id'>
					<td>{{offset + idx + 1}}</td>
					<td><a class='external' :href="'https://www.wikidata.org/wiki/Q'+row.q" target='_blank'>Q{{row.q}}</a></td>
					<td><a class='external' :href="'https://www.wikidata.org/wiki/Property:P'+row.prop" target='_blank'>P{{row.prop}}</a></td>
					<td><small v-html='row.html || row.message'></small></td>
					<td><span :style="row.status=='REOPENED' ? 'color:red' : ''">{{row.status}}</span></td>
					<td>{{getTypeLabel(row.type)}}</td>
					<td nowrap><small v-if='row.user'><a class='external' :href="'https://www.wikidata.org/wiki/User:'+encodeURIComponent(row.user)" target='_blank'>{{row.user}}</a></small></td>
					<td nowrap><small>{{prettyTime(row.timestamp)}}</small></td>
					<td>
						<button v-if='row.status!="DONE" && widar.is_logged_in' class='btn btn-dark btn-sm' @click.prevent='setStatus(row,"DONE")'>Done</button>
						<button v-else-if='row.status=="DONE" && widar.is_logged_in' class='btn btn-danger btn-sm' @click.prevent='setStatus(row,"REOPENED")'>Re-open</button>
						<small v-else-if='!widar.is_logged_in'><a href='/widar/index.php?action=authorize' target='_blank'>Log in</a></small>
					</td>
				</tr>
			</tbody>
		</table>
		<pagination v-if='total > limit' :offset='offset' :items-per-page='limit' :total='total'
			@go-to-page='goToPage'></pagination>
	</div>
</div>`
});

// ── Exports ─────────────────────────────────────────────────────────

export { LcCatalogList, LcReport, LcReportList, LcRecentChanges };
export default LcCatalogList;
