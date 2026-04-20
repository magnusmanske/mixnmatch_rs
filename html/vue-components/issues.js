import { mnm_api, mnm_notify, ensure_catalogs, get_specific_catalog, tt_update_interface, widar } from './store.js';

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

export default Vue.extend({
	props: ['type', 'initial_catalogs'],
	data: function () {
		return {
			limit: 25,
			page_size_options: PAGE_SIZE_OPTIONS,
			issues: [],
			entries: {},
			num_issues: 0,
			start: 0,
			available_types: ['WD_DUPLICATE', 'MISMATCH', 'MISMATCH_DATES', 'MULTIPLE'],
			total: -1,
			catalogs: '',
			loading: false,
			// Server-side seed for deterministic pagination — we lock to the
			// threshold the server picked on the *first* page load so paging
			// next/prev sees a consistent random ordering. `reload()` clears it
			// to ask for a fresh sample.
			random_threshold: null,
		};
	},
	created: function () {
		if (typeof this.initial_catalogs != 'undefined') this.catalogs = this.initial_catalogs;
		this.load();
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		load: async function () {
			let me = this;
			me.issues = [];
			me.loading = true;
			let the_type = (me.type || '');
			if (the_type == 'ALL') the_type = '';
			let params = {
				type: the_type,
				limit: me.limit,
				offset: (me.start || 0),
				catalogs: me.catalogs,
			};
			if (me.random_threshold !== null) params.random_threshold = me.random_threshold;
			try {
				let d = await mnm_api('get_issues', params);
				let entries = d.data.entries || {};
				Object.entries(entries).forEach(function ([k, v]) {
					if (typeof d.data.users[v.user] == 'undefined') return;
					entries[k].username = d.data.users[v.user].name;
				});
				let catalog_ids = [...new Set(Object.values(entries).map(function (e) { return e.catalog; }))];
				await ensure_catalogs(catalog_ids);
				me.total = (d.data.open_issues || 0) * 1;
				me.entries = entries;
				me.issues = Array.isArray(d.data.issues) ? d.data.issues : Object.values(d.data.issues || {});
				me.num_issues = me.issues.length;
				me.issues.forEach(function (v, k) {
					Vue.set(me.issues[k], 'is_resolved', false);
				});
				// Capture the seed so paging stays consistent until reload.
				if (me.random_threshold === null && typeof d.data.random_threshold == 'number') {
					me.random_threshold = d.data.random_threshold;
				}
			} catch (e) {
				mnm_notify(e.message || 'Failed to load issues', 'danger');
			} finally {
				me.loading = false;
			}
		},
		reload: function () {
			// Re-roll the random sample (next visit gets a different page-1).
			this.random_threshold = null;
			this.start = 0;
			this.load();
		},
		applyFilters: function () {
			this.start = 0;
			this.random_threshold = null;
			this.load();
		},
		changePageSize: function () {
			this.start = 0;
			this.load();
		},
		goToPage: function (offset) {
			this.start = offset;
			this.load();
			// Scroll back to the top so the new page is visible without manual scrolling.
			if (typeof window != 'undefined' && window.scrollTo) window.scrollTo(0, 0);
		},
		canResolve: function () {
			return typeof widar.getUserName() != 'undefined'
		},
		get_catalog: function (catalog_id) {
			return get_specific_catalog(catalog_id);
		},
		get_catalog_slash: function () {
			if (this.catalogs == '') return '';
			return '/' + this.catalogs;
		},
		typeLabel: function (t) {
			return (t || 'ALL').toLowerCase().replace(/_/g, ' ');
		},
		resolve: async function (issue_id) {
			let me = this;
			try {
				await mnm_api('resolve_issue', {
					issue_id: issue_id,
					username: widar.getUserName()
				});
				let idx = me.issues.findIndex(function (i) { return i.id === issue_id; });
				if (idx >= 0) Vue.set(me.issues[idx], 'is_resolved', true);
				me.total--;
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
		}
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{tt: 'issues'}]"></mnm-breadcrumb>

	<div class='d-flex flex-wrap align-items-center justify-content-between gap-2 mb-2'>
		<h1 class='mb-0' tt='issues'></h1>
		<div class='d-flex align-items-center gap-2'>
			<select v-model.number='limit' @change='changePageSize' class='form-select form-select-sm' style='width:auto' :disabled='loading'>
				<option v-for='n in page_size_options' :value='n'>{{n}}/page</option>
			</select>
			<button class='btn btn-outline-secondary btn-sm' :disabled='loading' @click.prevent='reload'>
				<span tt='reload'></span>
			</button>
		</div>
	</div>

	<p class='text-muted mb-2' tt='issues_blurb'></p>

	<div class='d-flex flex-wrap align-items-center gap-3 mb-2'>
		<div>
			<span tt='open_issues'></span>:
			<b v-if='total>-1'>{{total.toLocaleString()}}</b>
			<i v-else>—</i>
		</div>
		<div class='d-flex align-items-center gap-1'>
			<label class='form-label mb-0 small text-muted'><span tt='catalogs'></span>:</label>
			<input type='text' class='form-control form-control-sm' style='width:14rem'
				v-model='catalogs' @keyup.enter='applyFilters'
				placeholder='e.g. 1,42,99' />
			<button class='btn btn-outline-primary btn-sm' :disabled='loading' @click.prevent='applyFilters' tt='apply'>Apply</button>
		</div>
	</div>

	<ul class='nav nav-tabs mb-3'>
		<li class='nav-item'>
			<router-link class='nav-link' :class="{active: !type || type=='ALL'}"
				:to='"/issues/ALL"+get_catalog_slash()' tt='all_types'></router-link>
		</li>
		<li class='nav-item' v-for='possible_type in available_types' :key='possible_type'>
			<router-link class='nav-link' :class="{active: type==possible_type}"
				:to='"/issues/"+possible_type+get_catalog_slash()' :tt='possible_type.toLowerCase()'></router-link>
		</li>
	</ul>

	<div v-if='loading' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>⏳</div>
		<i tt='loading'></i>
	</div>
	<div v-else-if='num_issues==0' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>✓</div>
		<i tt='no_results'></i>
	</div>
	<div v-else>
		<pagination v-if='total > limit' :offset='start' :items-per-page='limit' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>

		<div v-for='(i,i_idx) in issues' class='card mb-3' :key='i.id' :class="{ 'border-success': i.is_resolved }">
			<div class='card-body'>
				<entry-list-item :show_catalog=1 :entry='entries[i.entry_id]' :show_permalink='1' :key='i.entry_id'></entry-list-item>
				<div class='d-flex flex-wrap align-items-start gap-2 mt-2'>
					<div v-if='i.is_resolved' class='text-success'>
						<span tt='resolved'></span> ✓
					</div>
					<div v-else-if='i.type=="MISMATCH"' class='flex-grow-1'>
						<h3 class='h6 mb-1' tt='mismatch'></h3>
						<span v-for='(q,q_idx) in i.json' :key='q_idx'>
							<span v-if='q_idx>0'>, </span>
							<wd-link :item='q.replace(/^\\{/,"")'></wd-link>
						</span>
					</div>
					<div v-else-if='i.type=="MISMATCH_DATES"' class='flex-grow-1'>
						<h3 class='h6 mb-1' tt='mismatch_dates'></h3>
						<table class='table table-sm mb-0' style='width:auto'>
							<tr>
								<th tt='toolname'></th>
								<td>{{i.json.mnm_time.replace(/T.*\$/,'').replace(/\\+/,'')}}</td>
							</tr>
							<tr>
								<th tt='wikidata'></th>
								<td>{{i.json.wd_time.replace(/T.*\$/,'').replace(/\\+/,'')}}</td>
							</tr>
						</table>
					</div>
					<div v-else-if='i.type=="WD_DUPLICATE"' class='flex-grow-1'>
						<h3 class='h6 mb-1' tt='wd_duplicate'></h3>
						<span v-for='(q,q_idx) in i.json' :key='q_idx'>
							<span v-if='q_idx>0'>, </span>
							<wd-link :item='q'></wd-link>
						</span>
					</div>
					<div v-else-if='i.type=="MULTIPLE"' class='flex-grow-1'>
						<h3 class='h6 mb-1' tt='multiple'></h3>
						<div class='small text-muted mb-1'><span tt='multiple_desc'></span></div>
						<table class='table table-sm mb-0' style='width:auto'>
							<tr v-if='get_catalog(entries[i.entry_id].catalog) && get_catalog(entries[i.entry_id].catalog).wd_prop!=null && get_catalog(entries[i.entry_id].catalog).wd_qual==null'>
								<th tt='property'></th>
								<td>
									<wd-link :item='"P"+get_catalog(entries[i.entry_id].catalog).wd_prop'></wd-link>
								</td>
							</tr>
							<tr>
								<th tt='wikidata'></th>
								<td>{{i.json.wd.join(', ')}}</td>
							</tr>
							<tr>
								<th tt='toolname'></th>
								<td>{{i.json.mnm}}</td>
							</tr>
						</table>
					</div>
					<div v-else class='flex-grow-1'>
						<h3 class='h6 mb-1'>{{i.type}}</h3>
						<pre class='small mb-0'>{{i.json}}</pre>
					</div>
					<div v-if='!i.is_resolved' class='ms-auto'>
						<button v-if='canResolve()' class='btn btn-outline-primary btn-sm' tt='resolve' @click.prevent='resolve(i.id)'></button>
						<small v-else class='text-muted' tt='log_into_widar'></small>
					</div>
				</div>
			</div>
		</div>

		<pagination v-if='total > limit' :offset='start' :items-per-page='limit' :total='total'
			@go-to-page='goToPage'></pagination>
	</div>
</div>
`
});
