import { mnm_api, mnm_notify, ensure_catalog, ensure_catalogs, get_specific_catalog, tt_update_interface } from './store.js';

const SINCE_OPTIONS = [
	{ value: '0', label: 'All' },
	{ value: '1', label: '24h' },
	{ value: '7', label: '7d' },
	{ value: '30', label: '30d' },
];

const PER_PAGE = 100;

(function () {
	// Toolbar + empty-state styles only — the event-list CSS lives with
	// the shared rc-events-list component.
	if (document.querySelector('style[data-mnm="rc-page"]')) return;
	const s = document.createElement('style');
	s.setAttribute('data-mnm', 'rc-page');
	s.textContent = `
.mnm-rc-toolbar { display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center;
	padding:0.5rem 0.75rem; border:1px solid var(--mnm-border,#dee2e6);
	border-radius:0.25rem; background:var(--mnm-bg-alt,#f4f6f8);
	margin-bottom:0.75rem; }
.mnm-rc-toolbar .mnm-rc-toolbar-spacer { flex:1 1 auto; }
.mnm-rc-toolbar .btn-group .btn { font-size:0.8125rem; }
.mnm-rc-meta { color:var(--mnm-text-muted,#6c757d); font-size:0.8125rem; }

.mnm-rc-empty { text-align:center; padding:2rem 1rem;
	color:var(--mnm-text-muted,#6c757d);
	border:1px dashed var(--mnm-border,#dee2e6); border-radius:0.25rem;
	background:var(--mnm-bg-alt,#f4f6f8); }
.mnm-rc-empty-icon { font-size:2rem; line-height:1; margin-bottom:0.5rem; }
`;
	document.head.appendChild(s);
})();

export default Vue.extend({
	props: ['catalog'],
	data: function () {
		return {
			entries: [],
			total: 0,
			offset: 0,
			per_page: PER_PAGE,
			since: '0',
			loading: true,
			catalog_obj: null,
			since_options: SINCE_OPTIONS,
		};
	},
	created: function () { this.loadData(); },
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	computed: {
		is_specific_catalog: function () { return typeof this.catalog !== 'undefined'; },
		atom_url: function () {
			const base = 'https://mix-n-match.toolforge.org/api.php?query=rc_atom';
			return this.is_specific_catalog ? (base + '&catalog=' + this.catalog) : base;
		},
	},
	methods: {
		getSinceTimestamp: function () {
			if (!this.since || this.since === '0') return '';
			const d = new Date();
			d.setDate(d.getDate() - parseInt(this.since));
			const pad = n => String(n).padStart(2, '0');
			return d.getFullYear() + pad(d.getMonth() + 1) + pad(d.getDate()) + '000000';
		},
		setSince: function (v) {
			if (this.since === v) return;
			this.since = v;
			this.offset = 0;
			this.loadData();
		},
		goToPage: function (newOffset) {
			this.offset = newOffset;
			this.loadData();
			if (typeof window !== 'undefined' && window.scrollTo) window.scrollTo(0, 0);
		},
		loadData: async function () {
			const me = this;
			me.loading = true;
			try {
				if (me.is_specific_catalog) {
					await ensure_catalog(me.catalog * 1);
					me.catalog_obj = get_specific_catalog(me.catalog * 1) || null;
				} else {
					me.catalog_obj = null;
				}
				const params = { limit: me.per_page, offset: me.offset };
				if (me.is_specific_catalog) params.catalog = me.catalog * 1;
				const ts = me.getSinceTimestamp();
				if (ts) params.ts = ts;
				const d = await mnm_api('rc', params);
				const events = d.data.events || [];
				Object.entries(events).forEach(function ([k, v]) {
					if (typeof d.data.users[v.user] === 'undefined') return;
					events[k].username = d.data.users[v.user].name;
				});
				const catalog_ids = [...new Set(
					Object.values(events).map(e => e.catalog).filter(Boolean)
				)];
				await ensure_catalogs(catalog_ids);
				me.entries = events;
				me.total = d.data.total || 0;
			} catch (e) {
				mnm_notify(e.message || 'Failed to load recent changes', 'danger');
			}
			me.loading = false;
		},
	},
	watch: {
		'$route'() { this.offset = 0; this.loadData(); },
	},
	template: `
	<div class='mt-2'>
		<mnm-breadcrumb v-if='is_specific_catalog && catalog_obj && catalog_obj.id' :crumbs="[
			{text: catalog_obj.name, to: '/catalog/'+catalog_obj.id},
			{tt: 'recent_changes'}
		]"></mnm-breadcrumb>
		<mnm-breadcrumb v-else :crumbs="[{tt: 'recent_changes'}]"></mnm-breadcrumb>

		<catalog-header v-if='is_specific_catalog && catalog_obj && catalog_obj.id'
			:catalog='catalog_obj'></catalog-header>
		<h1 v-else class='mb-3' tt='recent_changes'></h1>

		<!-- Toolbar: timeframe, refresh, atom link -->
		<div class='mnm-rc-toolbar'>
			<span class='mnm-rc-meta me-1'>Showing</span>
			<div class='btn-group' role='group' aria-label='Time range'>
				<button v-for='opt in since_options' :key='opt.value'
					type='button'
					class='btn btn-sm'
					:class="since === opt.value ? 'btn-primary' : 'btn-outline-secondary'"
					@click.prevent='setSince(opt.value)'>
					{{opt.label}}
				</button>
			</div>
			<span class='mnm-rc-meta'>&middot; {{total}} event{{total === 1 ? '' : 's'}}</span>

			<span class='mnm-rc-toolbar-spacer'></span>

			<button class='btn btn-sm btn-outline-secondary' @click.prevent='loadData' :disabled='loading'
				:title="'Reload'">
				<span v-if='loading' class='spinner-border spinner-border-sm' role='status' aria-hidden='true'></span>
				<span v-else>&#x21bb;</span>
				<span class='ms-1' tt='refresh'></span>
			</button>
			<a :href='atom_url' class='btn btn-sm btn-outline-secondary' target='_blank' rel='noopener'
				:title="'Atom feed'">
				<span aria-hidden='true'>\u{1F4E1}</span>
				<span class='ms-1' tt='recent_changes_atom_feed'></span>
			</a>
		</div>

		<pagination v-if='total > per_page' :offset='offset' :items-per-page='per_page' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>

		<!-- Loading / empty / list -->
		<div v-if='loading && entries.length === 0' class='mnm-rc-empty'>
			<div class='mnm-rc-empty-icon'>\u23f3</div>
			<i tt='loading'></i>
		</div>
		<div v-else-if='entries.length === 0' class='mnm-rc-empty'>
			<div class='mnm-rc-empty-icon'>\u{1F4ED}</div>
			<div tt='no_results'></div>
			<div class='small mt-1'>Try widening the time range.</div>
		</div>
		<rc-events-list v-else :events='entries' :show-catalog='!is_specific_catalog'></rc-events-list>

		<pagination v-if='total > per_page' :offset='offset' :items-per-page='per_page' :total='total'
			:show-first-last='true' @go-to-page='goToPage'></pagination>
	</div>
	`
});
