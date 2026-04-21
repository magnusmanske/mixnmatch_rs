import { mnm_api, mnm_notify, tt_update_interface, widar } from './store.js';

const PAGE_SIZE_OPTIONS = [25, 50, 100, 250];
const STATUS_LABELS = {
	'NO_CATALOG': 'No catalog',
	'HAS_CATALOG': 'Has catalog',
	'NOT_SUITABLE': 'Not suitable',
	'DIFFICULT': 'Difficult',
	'BROKEN': 'Broken',
};
const STATUS_BADGE = {
	'NO_CATALOG': 'bg-info text-white',
	'HAS_CATALOG': 'bg-success',
	'NOT_SUITABLE': 'bg-warning text-dark',
	'DIFFICULT': 'bg-secondary',
	'BROKEN': 'bg-danger',
};

(function () {
	// Page-local styles. Kept inline so the file stays self-contained.
	const s = document.createElement('style');
	s.textContent = `
.mnm-mp-toolbar { display:flex; flex-wrap:wrap; gap:0.75rem; align-items:center;
	padding:0.5rem; border:1px solid var(--mnm-border,#dee2e6); border-radius:0.25rem;
	background:var(--mnm-bg-alt,#f4f6f8); margin-bottom:0.75rem; }
.mnm-mp-toolbar > .group { display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center; }
.mnm-mp-toolbar label { margin-bottom:0; cursor:pointer; }
.mnm-mp-status-cell .badge { font-size:0.75rem; }
.mnm-mp-row.last-clicked { background-color:#fff3cd !important; }
.mnm-mp-progress { height:1rem; }
.mnm-mp-progress .progress-bar { font-size:0.7rem; line-height:1rem; }
`;
	document.head.appendChild(s);
})();

export default Vue.extend({
	data: function () {
		return {
			loaded: false, props: [], props_filtered: [], error: '',
			sort: 'property_name', sort_reverse: false,
			last_id_clicked_on: 0,
			start: 0, max: 50,
			page_size_options: PAGE_SIZE_OPTIONS,
			status_stats: {},
			status_labels: STATUS_LABELS,
			status_badge: STATUS_BADGE,
			statuses: { 'NO_CATALOG': true, 'HAS_CATALOG': false, 'NOT_SUITABLE': false, 'DIFFICULT': false, 'BROKEN': false },
			search: '',
		};
	},
	created: async function () {
		let self = this;
		try {
			let d = await mnm_api('get_missing_properties');
			self.props_filtered = [];
			self.status_stats = {};
			Object.keys(self.statuses).forEach(function (key) {
				self.status_stats[key] = 0;
			});
			self.props = d.data || [];
			self.props.forEach(function (row) {
				row.property_name_lc = (row.property_name || '').toLowerCase();
				row.property_num *= 1;
				if (typeof self.status_stats[row.status] !== 'undefined') self.status_stats[row.status] += 1;
			});
			self.filter_props();
		} catch (e) {
			self.error = e.message || 'Failed to load missing properties';
		} finally {
			self.loaded = true;
		}
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	computed: {
		widar: function () { return widar; },
		total: function () { return this.props_filtered.length; },
		rows: function () {
			return this.props_filtered.slice(this.start, this.start + this.max);
		},
	},
	methods: {
		statusLabel: function (status) {
			return this.status_labels[status] || status.toLowerCase().replace(/_/g, ' ');
		},
		percentOf: function (count) {
			if (!this.props.length) return 0;
			return Math.round(100 * count / this.props.length);
		},
		goToPage: function (new_offset) {
			this.start = new_offset;
			this.last_id_clicked_on = 0;
			if (typeof window != 'undefined' && window.scrollTo) window.scrollTo(0, 0);
		},
		was_clicked_on: function (row) {
			return this.last_id_clicked_on == row.id ? 'last-clicked' : '';
		},
		get_scraper_link: function (row) {
			let url = "https://mix-n-match.toolforge.org/#/scraper/new?property=P" + row.property_num;
			if (row.default_type == 'Q5') url += "&type=biography";
			return url;
		},
		clicked_on: function (row) {
			this.last_id_clicked_on = row.id;
		},
		on_status_change: async function (row) {
			let note = prompt("Add a note (optional):", row.note || "");
			if (note === null) note = '';
			row.note = note;
			try {
				await mnm_api('set_missing_properties_status', {
					row_id: row.id,
					status: row.status,
					note: note,
					username: widar.getUserName(),
				});
				// Recompute the per-status counts so the progress bar reflects the change.
				this.recount_statuses();
			} catch (e) {
				mnm_notify(e.message || 'Request failed', 'danger');
			}
		},
		recount_statuses: function () {
			let self = this;
			Object.keys(self.statuses).forEach(function (k) { self.status_stats[k] = 0; });
			self.props.forEach(function (row) {
				if (typeof self.status_stats[row.status] !== 'undefined') self.status_stats[row.status] += 1;
			});
			self.status_stats = Object.assign({}, self.status_stats);
			self.filter_props();
		},
		sort_list: function (row1, row2) {
			if (this.sort == 'property_name') {
				if (row1.property_name_lc === row2.property_name_lc) return 0;
				return row1.property_name_lc < row2.property_name_lc ? -1 : 1;
			}
			if (this.sort == 'property_id') {
				return row1.property_num - row2.property_num;
			}
			return 0;
		},
		filter_props: function () {
			let self = this;
			self.start = 0;
			let needle = (self.search || '').trim().toLowerCase();
			let cache = self.props.filter(function (row) {
				if (!self.statuses[row.status]) return false;
				if (!needle) return true;
				return (row.property_name_lc.indexOf(needle) !== -1)
					|| ('p' + row.property_num).indexOf(needle) === 0;
			});
			cache.sort(self.sort_list);
			if (self.sort_reverse) cache.reverse();
			self.props_filtered = cache;
			self.last_id_clicked_on = 0;
		},
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{text: 'Missing properties'}]"></mnm-breadcrumb>
	<h1>Missing properties</h1>
	<p class='text-muted'>Wikidata properties that don't yet have a Mix'n'match catalog. Pick a status to record progress.</p>

	<div v-if='!loaded' class='mnm-empty-state'>
		<div class='mnm-empty-icon'>⏳</div>
		<i tt='loading'></i>
	</div>
	<div v-else-if='error' class='alert alert-danger'>
		<b>Error:</b> {{error}}
	</div>
	<div v-else>
		<div class='mnm-mp-toolbar'>
			<div class='group'>
				<span class='small text-muted'>Show:</span>
				<label v-for='(value,status) in statuses' :key='status' class='small'>
					<input type='checkbox' v-model='statuses[status]' @change='filter_props' />
					<span class='badge ms-1' :class='status_badge[status]'>{{statusLabel(status)}}</span>
				</label>
			</div>
			<div class='group ms-auto'>
				<span class='small text-muted'>Sort by:</span>
				<label class='small'>
					<input type='radio' v-model='sort' value='property_name' @change='filter_props' />
					<span tt='by_alpha'></span>
				</label>
				<label class='small'>
					<input type='radio' v-model='sort' value='property_id' @change='filter_props' />
					<span tt='by_id'></span>
				</label>
				<label class='small'>
					<input type='checkbox' v-model='sort_reverse' @change='filter_props' />
					<span tt='descending'></span>
				</label>
			</div>
		</div>

		<div class='mnm-mp-toolbar'>
			<div class='group flex-grow-1'>
				<input type='text' class='form-control form-control-sm' style='max-width:18rem'
					v-model='search' @input='filter_props' placeholder='Search by P-number or name…' />
			</div>
			<div class='group'>
				<select v-model.number='max' @change='goToPage(0)' class='form-select form-select-sm' style='width:auto'>
					<option v-for='n in page_size_options' :value='n'>{{n}}/page</option>
				</select>
			</div>
		</div>

		<!-- Status distribution -->
		<div class='progress mnm-mp-progress mb-2' role='progressbar' aria-label='Status distribution'>
			<div v-for='(count,key) in status_stats' :key='key'
				:class="['progress-bar', status_badge[key]]"
				:style="{ width: (props.length ? (100*count/props.length) : 0) + '%' }"
				:title="statusLabel(key) + ': ' + count + ' (' + percentOf(count) + '%)'">
				<span v-if='percentOf(count) >= 6'>{{percentOf(count)}}%</span>
			</div>
		</div>

		<div v-if='rows.length>0'>
			<pagination :offset='start' :items-per-page='max' :total='total'
				:show-first-last='true' @go-to-page='goToPage'></pagination>

			<table class='table table-sm table-striped table-hover align-middle'>
				<thead class='table-light'>
					<tr>
						<th>Property</th>
						<th>Name</th>
						<th>Default type</th>
						<th>Status</th>
						<th>Action</th>
					</tr>
				</thead>
				<tbody>
					<tr v-for='row in rows' :key='row.id' :class='was_clicked_on(row)' @click='clicked_on(row)' style='cursor:pointer'>
						<td>
							<a :href='"https://www.wikidata.org/wiki/Property:P"+row.property_num' target='_blank' rel='noopener'
								class='wikidata' @click.stop>{{"P"+row.property_num}}</a>
						</td>
						<td>{{row.property_name}}</td>
						<td><small class='text-muted'>{{row.default_type}}</small></td>
						<td class='mnm-mp-status-cell'>
							<span v-if='widar.is_logged_in'>
								<select @change='on_status_change(row)' v-model='row.status' class='form-select form-select-sm' @click.stop>
									<option v-for='(value,status) in statuses' :key='status' :value='status'>
										{{statusLabel(status)}}
									</option>
								</select>
							</span>
							<span v-else>
								<span class='badge' :class='status_badge[row.status]'>{{statusLabel(row.status)}}</span>
							</span>
							<small class='d-block text-muted mt-1' v-if='row.note'>{{row.note}}</small>
						</td>
						<td>
							<a :href='get_scraper_link(row)' target='_blank' rel='noopener'
								class='btn btn-outline-primary btn-sm' @click.stop tt='new_scraper'>+ New scraper</a>
						</td>
					</tr>
				</tbody>
			</table>

			<pagination :offset='start' :items-per-page='max' :total='total'
				@go-to-page='goToPage'></pagination>
		</div>
		<div v-else class='mnm-empty-state'>
			<div class='mnm-empty-icon'>🔍</div>
			<i>No properties match the current filters.</i>
		</div>
	</div>
</div>
`
});
