import { mnm_api, mnm_notify, ensure_catalog, ensure_catalogs, get_specific_catalog, tt_update_interface } from './store.js';

const SINCE_OPTIONS = [
	{ value: '0', label: 'All' },
	{ value: '1', label: '24h' },
	{ value: '7', label: '7d' },
	{ value: '30', label: '30d' },
];

(function () {
	// Page-local styles. Kept inline so the file stays self-contained —
	// same pattern as missing_properties.js and large_catalogs.js.
	const s = document.createElement('style');
	s.textContent = `
.mnm-rc-toolbar { display:flex; flex-wrap:wrap; gap:0.5rem; align-items:center;
	padding:0.5rem 0.75rem; border:1px solid var(--mnm-border,#dee2e6);
	border-radius:0.25rem; background:var(--mnm-bg-alt,#f4f6f8);
	margin-bottom:0.75rem; }
.mnm-rc-toolbar .mnm-rc-toolbar-spacer { flex:1 1 auto; }
.mnm-rc-toolbar .btn-group .btn { font-size:0.8125rem; }
.mnm-rc-meta { color:var(--mnm-text-muted,#6c757d); font-size:0.8125rem; }

.mnm-rc-list { display:flex; flex-direction:column; gap:0; border:1px solid var(--mnm-border,#dee2e6);
	border-radius:0.25rem; overflow:hidden; background:#fff; }
.mnm-rc-row { display:grid;
	grid-template-columns: 1.75rem minmax(8rem,11rem) minmax(10rem,1.25fr) minmax(14rem,1.5fr) minmax(7rem,0.9fr);
	gap:0.75rem; align-items:center; padding:0.45rem 0.75rem;
	border-bottom:1px solid #f0f2f4; }
.mnm-rc-row:last-child { border-bottom:0; }
.mnm-rc-row:hover { background:#f8fafc; }

.mnm-rc-icon { display:inline-flex; align-items:center; justify-content:center;
	width:1.5rem; height:1.5rem; border-radius:50%; font-size:0.85rem; font-weight:600;
	line-height:1; }
.mnm-rc-icon-match   { background:#e0f5ec; color:#14866d; }
.mnm-rc-icon-na      { background:#fff8d9; color:#9a7a00; }
.mnm-rc-icon-remove  { background:#fde2e2; color:#b32424; }

.mnm-rc-time { font-variant-numeric:tabular-nums; font-size:0.8125rem;
	color:var(--mnm-text-muted,#6c757d); line-height:1.25; }
.mnm-rc-time strong { display:block; color:var(--mnm-text,#202122); font-weight:600; font-size:0.875rem; }

.mnm-rc-entry { min-width:0; }
.mnm-rc-entry-name { font-weight:500; }
.mnm-rc-entry-name a { color:var(--mnm-text,#202122); }
.mnm-rc-entry-name a:hover { color:var(--mnm-blue,#36c); }
.mnm-rc-entry-meta { color:var(--mnm-text-muted,#6c757d); font-size:0.75rem;
	overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.mnm-rc-entry-meta code { font-size:0.72rem; }
.mnm-rc-permalink { color:var(--mnm-text-muted,#6c757d); text-decoration:none; margin-right:0.25rem; }
.mnm-rc-permalink:hover { color:var(--mnm-blue,#36c); }

.mnm-rc-event { font-size:0.8125rem; min-width:0; }
.mnm-rc-event-label { color:var(--mnm-text-muted,#6c757d); margin-right:0.3rem; }
.mnm-rc-event-remove { color:var(--mnm-red,#b32424); }
.mnm-rc-event-na { color:#9a7a00; }

.mnm-rc-user { text-align:right; font-size:0.8125rem;
	color:var(--mnm-text-muted,#6c757d);
	overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }

.mnm-rc-empty { text-align:center; padding:2rem 1rem;
	color:var(--mnm-text-muted,#6c757d);
	border:1px dashed var(--mnm-border,#dee2e6); border-radius:0.25rem;
	background:var(--mnm-bg-alt,#f4f6f8); }
.mnm-rc-empty-icon { font-size:2rem; line-height:1; margin-bottom:0.5rem; }

@media (max-width: 767.98px) {
	.mnm-rc-row {
		grid-template-columns: 1.5rem 1fr;
		grid-template-areas:
			"icon time"
			"icon entry"
			"icon event"
			"icon user";
		row-gap:0.15rem;
		padding:0.6rem 0.75rem;
	}
	.mnm-rc-row > .mnm-rc-icon { grid-area:icon; align-self:start; margin-top:0.15rem; }
	.mnm-rc-row > .mnm-rc-time  { grid-area:time; }
	.mnm-rc-row > .mnm-rc-entry { grid-area:entry; }
	.mnm-rc-row > .mnm-rc-event { grid-area:event; }
	.mnm-rc-row > .mnm-rc-user  { grid-area:user; text-align:left; }
}
`;
	document.head.appendChild(s);
})();

function formatAbsoluteTimestamp(ts) {
	if (!ts || ts.length < 14) return '';
	return ts.substr(0, 4) + '-' + ts.substr(4, 2) + '-' + ts.substr(6, 2)
		+ ' ' + ts.substr(8, 2) + ':' + ts.substr(10, 2) + ':' + ts.substr(12, 2) + ' UTC';
}

// Parse a MnM `YYYYMMDDHHMMSS` timestamp and return a Date in UTC.
function parseTimestamp(ts) {
	if (!ts || ts.length < 14) return null;
	const iso = ts.substr(0, 4) + '-' + ts.substr(4, 2) + '-' + ts.substr(6, 2)
		+ 'T' + ts.substr(8, 2) + ':' + ts.substr(10, 2) + ':' + ts.substr(12, 2) + 'Z';
	const d = new Date(iso);
	return isNaN(d.getTime()) ? null : d;
}

function formatRelative(ts, now) {
	const d = parseTimestamp(ts);
	if (!d) return '';
	const sec = Math.max(0, Math.round((now.getTime() - d.getTime()) / 1000));
	if (sec < 60) return 'just now';
	const min = Math.round(sec / 60);
	if (min < 60) return min + ' min ago';
	const hr = Math.round(min / 60);
	if (hr < 24) return hr + ' hr ago';
	const day = Math.round(hr / 24);
	if (day < 30) return day + ' day' + (day === 1 ? '' : 's') + ' ago';
	const mon = Math.round(day / 30);
	if (mon < 12) return mon + ' mo ago';
	const yr = Math.round(day / 365);
	return yr + ' yr ago';
}

function formatTimeOnly(ts) {
	if (!ts || ts.length < 14) return '';
	return ts.substr(8, 2) + ':' + ts.substr(10, 2);
}

function formatDateShort(ts) {
	if (!ts || ts.length < 14) return '';
	return ts.substr(0, 4) + '-' + ts.substr(4, 2) + '-' + ts.substr(6, 2);
}

export default Vue.extend({
	props: ['catalog'],
	data: function () {
		return {
			entries: [],
			since: '0',
			loading: true,
			catalog_obj: null,
			now: new Date(),
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
			this.loadData();
		},
		loadData: async function () {
			const me = this;
			me.loading = true;
			me.now = new Date();
			try {
				if (me.is_specific_catalog) {
					await ensure_catalog(me.catalog * 1);
					me.catalog_obj = get_specific_catalog(me.catalog * 1) || null;
				} else {
					me.catalog_obj = null;
				}
				const params = {};
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
			} catch (e) {
				mnm_notify(e.message || 'Failed to load recent changes', 'danger');
			}
			me.loading = false;
		},
		entry_catalog_name: function (entry) {
			const c = get_specific_catalog(entry.catalog);
			return c && c.name ? c.name : '#' + entry.catalog;
		},
		event_kind: function (entry) {
			if (entry.event_type === 'remove_q') return 'remove';
			if (entry.event_type === 'match' && entry.q === 0) return 'na';
			return 'match';
		},
		event_icon: function (entry) {
			const k = this.event_kind(entry);
			if (k === 'remove') return '\u2715'; // ✕
			if (k === 'na') return 'N/A';
			return '\u2713'; // ✓
		},
		relative_time: function (ts) { return formatRelative(ts, this.now); },
		absolute_time: function (ts) { return formatAbsoluteTimestamp(ts); },
		date_short:    function (ts) { return formatDateShort(ts); },
		time_short:    function (ts) { return formatTimeOnly(ts); },
	},
	watch: {
		'$route'() { this.loadData(); },
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
			<span class='mnm-rc-meta'>&middot; {{entries.length}} event{{entries.length === 1 ? '' : 's'}}</span>

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
		<div v-else class='mnm-rc-list'>
			<div v-for='e in entries' :key='e.id + "-" + e.timestamp + "-" + e.event_type'
				class='mnm-rc-row'>
				<span class='mnm-rc-icon'
					:class="'mnm-rc-icon-' + event_kind(e)"
					:title="event_kind(e) === 'remove' ? 'Match removed' : event_kind(e) === 'na' ? 'Marked not applicable' : 'Matched'"
					aria-hidden='true'>{{event_icon(e)}}</span>

				<span class='mnm-rc-time' :title='absolute_time(e.timestamp)'>
					<strong>{{relative_time(e.timestamp)}}</strong>
					<span>{{date_short(e.timestamp)}} {{time_short(e.timestamp)}}</span>
				</span>

				<div class='mnm-rc-entry'>
					<div class='mnm-rc-entry-name'>
						<router-link :to='"/entry/"+e.id' class='mnm-rc-permalink' title='Entry detail'>#</router-link>
						<entry-link :entry='e'></entry-link>
					</div>
					<div class='mnm-rc-entry-meta' :title='e.ext_id'>
						<router-link v-if='!is_specific_catalog' :to='"/catalog/"+e.catalog'>{{entry_catalog_name(e)}}</router-link>
						<span v-if='!is_specific_catalog' class='mx-1'>&middot;</span>
						<code>{{e.ext_id}}</code>
					</div>
				</div>

				<div class='mnm-rc-event'>
					<template v-if="e.event_type === 'match'">
						<template v-if='e.q === 0'>
							<span class='mnm-rc-event-label' tt='matched_to'></span>
							<span class='mnm-rc-event-na' tt='not_applicable'></span>
						</template>
						<template v-else>
							<span class='mnm-rc-event-label' tt='matched_to'></span>
							<wd-link :item='e.q' :key='e.q'></wd-link>
						</template>
					</template>
					<span v-else-if="e.event_type === 'remove_q'" class='mnm-rc-event-remove' tt='wikidata_was_unlinked'></span>
				</div>

				<div class='mnm-rc-user'>
					<userlink :username='e.username' :user_id='e.user' />
				</div>
			</div>
		</div>
	</div>
	`
});
