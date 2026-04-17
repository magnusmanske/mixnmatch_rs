/**
 * MnM Vue 2 mixins for ES6 modules.
 */
import {
	mnm_api, mnm_notify, get_specific_catalog, widar,
	filteredEntryName, buildSearchString, decodeEntities,
	removeTags, miscFixes, pipe2newline, padDigits
} from './store.js';

export const entryMixin = {
	methods: {
		filteredName: function () { return filteredEntryName(this.entry.ext_name); },
		getSearchString: function (add_date) { return buildSearchString(this.entry, add_date); },
		get_catalog: function (catalog_id) { return get_specific_catalog(catalog_id); }
	}
};

export const entryDisplayMixin = {
	filters: {
		decodeEntities: function (s) { return decodeEntities(s); },
		removeTags: function (s) { return removeTags(s); },
		miscFixes: function (s) { return miscFixes(s); },
		pipe2newline: function (s) { return pipe2newline(s); },
	},
};

export const editEntryMixin = {
	data: function () { return { last_created_q: 0 } },
	methods: {
		digits: function (v, d) { return padDigits(v, d); },
		setEntryEdit: function (entry) {
			var d = new Date();
			var ts = '' + d.getFullYear() +
				padDigits(d.getMonth() + 1, 2) +
				padDigits(d.getDate(), 2) +
				padDigits(d.getHours(), 2) +
				padDigits(d.getMinutes(), 2) +
				padDigits(d.getSeconds(), 2);

			entry.username = widar.getUserName();
			entry.user = null;
			entry.timestamp = ts;
		},
		setEntryQ: function (entry, q, skip_wikidata_edit, callback, callback_fail, options) {
			const me = this;
			options = options || {};
			q = (('' + q).replace(/\D/g, '')) * 1;
			if (q <= 0) skip_wikidata_edit = true;

			var running = 1;
			function fin() {
				running--;
				if (running > 0) return;
				me.setEntryEdit(entry);
				entry.q = q;
				if (q > 0 && !options.silent) mnm_notify('Matched to Q' + q, 'success');
				if (typeof callback !== 'undefined') callback(q);
			}

			mnm_api('match_q', { tusc_user: widar.getUserName(), entry: entry.id, q: q }, { method: 'POST' })
				.then(function () { fin(); })
				.catch(function (e) {
					mnm_notify(e.message, 'danger');
					if (typeof callback_fail === 'function') callback_fail();
				});

			if (skip_wikidata_edit) return false;
			var catalog = get_specific_catalog(entry.catalog);
			if (!catalog || catalog.wd_prop == null) return false;
			if (catalog.wd_qual != null) return false;

			running++;
			var summary = 'Matched to [[:toollabs:mix-n-match/#/entry/' + entry.id + '|' + entry.ext_name + ' (#' + entry.id + ')]] in [[:toollabs:mix-n-match/#/catalog/' + catalog.id + '|' + catalog.name + ']]';
			widar.run({ botmode: 1, action: 'set_string', id: 'Q' + q, prop: 'P' + catalog.wd_prop, text: entry.ext_id, summary: summary }, function (d) {
				if (d.error != 'OK') {
					mnm_notify(d.error, 'danger');
					if (typeof callback_fail === 'function') callback_fail();
					return;
				}
				fin();
			});
		},
		removeAllMultimatches: async function (entry) {
			try {
				await mnm_api('remove_all_multimatches', { tusc_user: widar.getUserName(), entry: entry.id }, { method: 'POST' });
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
		},

		date2statement: function (prop, d) {
			var precision = 9;
			var m;
			var year;
			var month = '01';
			var day = '01';

			m = d.match(/^(\d+)-(\d+)-(\d+)$/);
			if (m != null) {
				precision = 11;
				year = '' + (m[1] * 1);
				month = '' + (m[2] * 1);
				day = '' + (m[3] * 1);
			} else {
				m = d.match(/^(\d+)-(\d+)$/);
				if (m != null) {
					precision = 10;
					year = '' + (m[1] * 1);
					month = '' + (m[2] * 1);
				} else {
					m = d.match(/^(\d+)$/);
					if (m != null) {
						precision = 9;
						year = '' + (m[1] * 1);
					} else return;
				}
			}

			if (month.length == 1) month = '0' + month;
			if (day.length == 1) day = '0' + day;
			var t = '+' + year + '-' + month + '-' + day + 'T00:00:00Z';

			return {
				mainsnak: {
					snaktype: 'value',
					property: prop,
					datavalue: {
						value: {
							'time': t,
							'timezone': 0,
							'before': 0,
							'after': 0,
							'precision': precision,
							'calendarmodel': 'http://www.wikidata.org/entity/Q1985727'
						},
						type: 'time'
					},
					datatype: 'time'
				},
				type: 'statement',
				rank: 'normal'
			};
		},

		getItemClaim: function (p, q) {
			return {
				mainsnak: {
					snaktype: 'value',
					property: 'P' + ('' + p).replace(/\D/g, ''),
					datavalue: {
						value: {
							'entity-type': 'item',
							'numeric-id': ('' + q).replace(/\D/g, '') * 1,
							'id': 'Q' + ('' + q).replace(/\D/g, '')
						},
						type: 'wikibase-entityid'
					},
					datatype: 'wikibase-item'
				},
				type: 'statement',
				rank: 'normal'
			};
		},

		newItemForEntry: async function (entry, callback, other_statements, default_entry, options) {
			const me = this;
			other_statements = other_statements || {};
			default_entry = default_entry || 0;
			var entry_ids = [];
			var first_entry;
			if (typeof entry.id === 'undefined') {
				Object.values(entry).forEach(function (e) {
					entry_ids.push(e.id);
					if (!first_entry) first_entry = e;
				});
			} else {
				entry_ids.push(entry.id);
				first_entry = entry;
			}
			try {
				var d = await mnm_api('prep_new_item', { entry_ids: entry_ids.join(','), default_entry: default_entry });
				if (typeof d.data === 'undefined') throw new Error(d.status || 'No data');
			} catch (e) {
				mnm_notify('Problem creating item: ' + e.message, 'danger');
				callback();
				return;
			}
			var first_catalog = get_specific_catalog(first_entry.catalog);
			var summary = 'New item based on [[:toollabs:mix-n-match/#/entry/' + first_entry.id + '|' + first_entry.ext_name + ' (#' + first_entry.id + ')]]' + (first_catalog ? ' in [[:toollabs:mix-n-match/#/catalog/' + first_catalog.id + '|' + first_catalog.name + ']]' : '');
			widar.run({
				action: 'generic', summary: summary,
				json: JSON.stringify({ action: 'wbeditentity', 'new': 'item', data: d.data })
			}, function (d) {
				if (d.error != 'OK') { mnm_notify(d.error, 'danger'); return; }
				var q = d.res.entity.id.replace(/\D/g, '');
				if (!q || q == 0) return;
				me.last_created_q = q;
				me.setEntryQ(first_entry, q, true, callback, undefined, options);
			});
		},
		confirmEntryQ: function (entry, callback) {
			var cat = get_specific_catalog(entry.catalog);
			var do_skip = !cat || cat.wd_prop == null || cat.wd_qual != null;
			this.setEntryQ(entry, entry.q, do_skip, callback);
		},
		removeEntryQ: async function (entry, callback) {
			if (entry.user > 0 && !confirm('Remove this manual match (Q' + entry.q + ')?')) return;
			try {
				await mnm_api('remove_q', { tusc_user: widar.getUserName(), entry: entry.id }, { method: 'POST' });
				entry.q = null; entry.user = null; entry.username = null; entry.timestamp = null;
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
			if (typeof callback !== 'undefined') callback();
		},
		removeEntryAllQ: async function (entry, callback) {
			if (entry.user > 0 && !confirm('Remove all matches for this entry?')) return;
			try {
				await mnm_api('remove_all_q', { tusc_user: widar.getUserName(), entry: entry.id }, { method: 'POST' });
				entry.q = null; entry.user = null; entry.username = null; entry.timestamp = null;
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
			if (typeof callback !== 'undefined') callback();
		}
	}
};
