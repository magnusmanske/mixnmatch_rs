/**
 * MnM shared store — centralized state and utilities.
 *
 * This module is the single source of truth for:
 *   - Catalog cache (specific + all)
 *   - API fetch wrappers (mnm_api, mnm_fetch_json)
 *   - Text/display utility functions
 *   - Translation interface helpers
 *   - Loading spinner management
 *   - Toast notifications
 *
 * Components should import from here rather than relying on window globals.
 * The globals are kept as thin aliases in index_vue.js for backward compatibility.
 */

// ── Reactive state (Vue.observable for Vue 2 reactivity) ─────────────

const state = Vue.observable({
	api: './api.php',
	all_catalogs_cache: undefined,
	specific_catalogs_cache: {},
	// Singleton instances — stored in reactive state so Vue templates
	// can access them via the global mixin below.
	wd: null,
	tt: null,
	widar: null,
});

// Expose for index_vue.js to create window.specific_catalogs_cache alias
export { state as _state };

// Direct reference to the reactive catalog cache (for catalog_group.js etc.)
export var specific_catalogs_cache = state.specific_catalogs_cache;

// ── Shared singleton instances ──────────────────────────────────────
// Module-level aliases updated by the setters below.  Method code uses
// the ES6 live-binding import; template code uses the Vue mixin which
// reads from the reactive `state` object.

export var wd = null;
export var tt = null;
export var widar = null;

export function setWd(w) { wd = w; state.wd = w; }
export function setTt(t) { tt = t; state.tt = t; }
export function setWidar(w) { widar = w; state.widar = w; }

// Make wd/tt/widar available in every component's template as
// reactive computed properties (templates can't see module imports).
Vue.mixin({
	computed: {
		wd: function () { return state.wd; },
		tt: function () { return state.tt; },
		widar: function () { return state.widar; },
	}
});

// ── API fetch utilities ──────────────────────────────────────────────

export async function mnm_api(query, params, options) {
	params = params || {};
	options = options || {};
	var method = (options.method || 'GET').toUpperCase();
	var url = state.api;

	if (method === 'GET') {
		var qs = new URLSearchParams(Object.assign({ query: query }, params));
		url += '?' + qs.toString();
		var resp = await fetch(url);
	} else {
		var form = new URLSearchParams(Object.assign({ query: query }, params));
		var resp = await fetch(url, { method: method, body: form, headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
	}

	if (!resp.ok) throw new Error('HTTP ' + resp.status + ' ' + resp.statusText);
	var json = await resp.json();
	if (json.status && json.status !== 'OK') throw new Error(json.status);
	return json;
}

export async function mnm_fetch_json(url, params) {
	if (params) {
		var qs = new URLSearchParams(params);
		url += (url.includes('?') ? '&' : '?') + qs.toString();
	}
	var resp = await fetch(url);
	if (resp.status === 429) {
		var retryAfter = Math.min(parseInt(resp.headers.get('Retry-After') || '10', 10), 60);
		mnm_notify('Rate limited by Wikidata; retrying in ' + retryAfter + 's\u2026', 'warning', (retryAfter + 2) * 1000);
		await new Promise(function (r) { setTimeout(r, retryAfter * 1000); });
		resp = await fetch(url);
	}
	if (!resp.ok) throw new Error('HTTP ' + resp.status);
	return resp.json();
}

// ── Toast notifications ──────────────────────────────────────────────

export function mnm_notify(message, type, delay) {
	type = type || 'info';
	delay = (typeof delay === 'undefined') ? 4000 : delay;
	var container = document.getElementById('mnm-toast-container');
	if (!container) return;
	var id = 'mnm-toast-' + Date.now();
	var autohide = delay > 0 ? 'true' : 'false';
	var icon = { success: '&#x2713;', danger: '&#x2715;', warning: '&#x26A0;', info: '&#x2139;' }[type] || '&#x2139;';
	message = message.replace(/\b(Q\d+)\b/g, '<a href="https://www.wikidata.org/wiki/$1" target="_blank" style="color:inherit;text-decoration:underline">$1</a>');
	var html =
		'<div id="' + id + '" class="toast align-items-center text-bg-' + type + ' border-0 mb-2" ' +
		'role="alert" aria-live="assertive" aria-atomic="true" ' +
		'data-bs-delay="' + delay + '" data-bs-autohide="' + autohide + '">' +
		'<div class="d-flex">' +
		'<div class="toast-body">' + icon + '&nbsp;' + message + '</div>' +
		'<button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>' +
		'</div></div>';
	container.insertAdjacentHTML('beforeend', html);
	var el = document.getElementById(id);
	var toast = new bootstrap.Toast(el);
	toast.show();
	el.addEventListener('hidden.bs.toast', function () { el.remove(); });
}

// ── Loading spinner ──────────────────────────────────────────────────

var _mnm_loading_count = 0;
export function mnm_loading(show) {
	_mnm_loading_count += show ? 1 : -1;
	if (_mnm_loading_count < 0) _mnm_loading_count = 0;
	var el = document.getElementById('mnm-navbar-loading');
	if (el) el.style.display = _mnm_loading_count > 0 ? '' : 'none';
}

// ── Translation interface (debounced) ────────────────────────────────

var _tt_update_scheduled = false;
export function tt_update_interface() {
	if (_tt_update_scheduled) return;
	_tt_update_scheduled = true;
	requestAnimationFrame(function () {
		_tt_update_scheduled = false;
		if (tt && tt.updateInterface) tt.updateInterface(document);
	});
}

// ── Catalog cache ────────────────────────────────────────────────────

export function format_all_catalogs(d) {
	var numFields = ['total', 'manual', 'autoq', 'nowd', 'noq', 'na'];
	Object.keys(d.data).forEach(function (k) {
		var v = d.data[k];
		numFields.forEach(function (f) { v[f] = Number(v[f]) || 0; });
		v.unmatched = v.total - v.manual - v.autoq - v.nowd - v.na;
	});
	return d.data;
}

var _catalogs_ready_resolve;
var _catalogs_ready = new Promise(function (resolve) { _catalogs_ready_resolve = resolve; });
var _catalogs_fetch_started = false;

async function _start_catalogs_fetch() {
	if (_catalogs_fetch_started) return;
	_catalogs_fetch_started = true;
	mnm_loading(true);
	try {
		var d = await mnm_api('catalogs');
		state.all_catalogs_cache = format_all_catalogs(d);
	} catch (e) {
		console.error('Failed to load catalogs', e);
	}
	mnm_loading(false);
	_catalogs_ready_resolve();
}

export function get_all_catalogs(callback) {
	if (typeof state.all_catalogs_cache !== 'undefined') {
		if (typeof callback !== 'undefined') return callback(state.all_catalogs_cache);
		return state.all_catalogs_cache;
	}
	_start_catalogs_fetch();
	if (typeof callback !== 'undefined') {
		_catalogs_ready.then(function () { callback(state.all_catalogs_cache); });
		return;
	}
	return {};
}

export function get_specific_catalog(catalog_id, force_reload) {
	if (state.specific_catalogs_cache[catalog_id] && !force_reload) return state.specific_catalogs_cache[catalog_id];
	if (typeof state.all_catalogs_cache !== 'undefined' && state.all_catalogs_cache[catalog_id]) return state.all_catalogs_cache[catalog_id];
	return undefined;
}

export async function ensure_catalog(catalog_id, force_reload) {
	if (!force_reload) {
		if (state.specific_catalogs_cache[catalog_id]) return;
	}
	try {
		var d = await mnm_api('single_catalog', { catalog_id: catalog_id });
		state.specific_catalogs_cache[catalog_id] = format_all_catalogs(d)[catalog_id];
	} catch (e) {
		console.error('Failed to load catalog ' + catalog_id + ':', e.message);
	}
}

export async function ensure_catalogs(catalog_ids, force_reload) {
	var uncached = [];
	catalog_ids.forEach(function (id) {
		id = id * 1;
		if (id > 0 && (force_reload || !state.specific_catalogs_cache[id])) uncached.push(id);
	});
	if (uncached.length === 0) return;
	if (uncached.length === 1) return ensure_catalog(uncached[0], force_reload);
	try {
		var d = await mnm_api('batch_catalogs', { catalog_ids: uncached.join(',') });
		var formatted = format_all_catalogs(d);
		Object.keys(formatted).forEach(function (k) {
			state.specific_catalogs_cache[k] = formatted[k];
		});
	} catch (e) {
		console.error('Failed to batch-load catalogs:', e.message);
	}
}

// ── Text utilities ───────────────────────────────────────────────────

export function escapeHtml(str) {
	if (!str) return '';
	var div = document.createElement('div');
	div.appendChild(document.createTextNode(str));
	return div.innerHTML;
}

export function filteredEntryName(ext_name) {
	if (!ext_name) return '';
	return ext_name.replace(/^(Sir|Madam|Madame|Saint) /, '').replace(/\s*\(.+?\)\s*/, ' ');
}

export function buildSearchString(entry, add_date_if_possible) {
	if (add_date_if_possible === undefined) add_date_if_possible = true;
	var ret = filteredEntryName(entry.ext_name);
	if (!ret) return '';
	ret = ret.replace(/\s*\(.+?\)\s*/g, ' ').replace(/\s*\[.+?\]\s*/g, ' ');
	ret = ret.replace(/\s+([A-Z]\s+)+/g, ' ').replace(/^[A-Z]\.{0,1} /, '');
	if (entry.type == 'Q5' && add_date_if_possible) {
		var m = (entry.ext_desc || '').match(/\b\d{3,4}\b/g);
		if (m) {
			m = m.map(Number).sort(function (a, b) { return a - b; });
			while (m.length > 1 && m[0] + 150 < m[1]) m.shift();
			if (m.length == 1) ret += ' ' + m[0];
			else if (m.length > 1) ret += ' ' + m[0] + ' ' + m[m.length - 1];
		}
	}
	return encodeURIComponent(ret);
}

var _decodeEntitiesEl;
export function decodeEntities(encodedString) {
	if (!_decodeEntitiesEl) _decodeEntitiesEl = document.createElement('textarea');
	_decodeEntitiesEl.innerHTML = encodedString;
	return _decodeEntitiesEl.value;
}

export function removeTags(input) { return (input || '').replace(/<.+?>/g, ''); }
export function miscFixes(input) { return (input || '').replace(/\\\\/g, ''); }
export function pipe2newline(input) { return (input || '').replace(/\|/g, '\n'); }
export function padDigits(v, digits) { v = '' + v; while (v.length < digits) v = '0' + v; return v; }

// ── Vue mixin for common component boilerplate ──────────────────────

/**
 * Shared mixin that provides the translation update hooks every component needs.
 * Usage: mixins: [mnmComponentMixin]
 */
export const mnmComponentMixin = {
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
};
