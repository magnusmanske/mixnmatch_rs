import { get_specific_catalog, tt_update_interface } from './store.js';

(function () {
	// Page-local styles. Loaded once; if another consumer already injected
	// them we re-inject anyway — the class names are scoped under .mnm-rc-*
	// so double-injection is harmless but wasteful; the guard keeps the
	// DOM clean.
	if (document.querySelector('style[data-mnm="rc-events-list"]')) return;
	const s = document.createElement('style');
	s.setAttribute('data-mnm', 'rc-events-list');
	s.textContent = `
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

export default {
	name: 'rc-events-list',
	props: {
		// An array of MnM RC events. Each event is expected to have
		// id, catalog, ext_id, ext_url, ext_name, q (number or null),
		// user (number), username (string, added client-side), timestamp
		// (YYYYMMDDHHMMSS UTC), and event_type ('match' | 'remove_q' | …).
		events: { type: Array, required: true },
		// Show the catalog name+link in the per-event meta line.
		// False when the outer page is already scoped to one catalog.
		showCatalog: { type: Boolean, default: true },
	},
	data: function () {
		return { now: new Date() };
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
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
	template: `
	<div class='mnm-rc-list'>
		<div v-for='e in events' :key='e.id + "-" + e.timestamp + "-" + e.event_type'
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
					<router-link v-if='showCatalog' :to='"/catalog/"+e.catalog'>{{entry_catalog_name(e)}}</router-link>
					<span v-if='showCatalog' class='mx-1'>&middot;</span>
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
	`
};
