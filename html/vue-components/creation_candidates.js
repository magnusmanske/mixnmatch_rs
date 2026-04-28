import { editEntryMixin } from './mnm-mixins.js';
import { mnm_api, mnm_fetch_json, mnm_notify, ensure_catalogs, tt_update_interface, widar } from './store.js';

export default Vue.extend({
	mixins: [editEntryMixin],
	props: ['mode'],
	data: function () { return { entries: [], edits_todo: [], grouped_entries: [], loaded: false, loaded_wd: false, wd_entries: [], has_commons_search_results: false, require_catalogs: [], min_catalogs_required: 0, require_catalogs_string: '', ext_name: '', birth_year: '', death_year: '', prop: '', group_by_birth_year: true, can_group_by_birth_year: false, checkbox_generation: 0, assign_q_input: {}, assigning_q: {} } },
	created: function () { this.loadData(); },
	updated: function () { tt_update_interface() }, //  $('.next_cc_set').focus() ;
	mounted: function () { tt_update_interface(); var _ncc = document.querySelector('.next_cc_set'); if (_ncc) _ncc.focus(); },
	methods: {
		loadData: async function () {
			const me = this;
			let min = 0;
			if (me.mode == 'human') min = 4;
			if (me.mode == 'dates') min = 2;
			if (typeof me.$route.query.min != 'undefined') min = me.$route.query.min;
			if (typeof me.$route.query.ext_name != 'undefined') me.ext_name = me.$route.query.ext_name;
			else me.ext_name = '';
			if (typeof me.$route.query.by != 'undefined') me.birth_year = me.$route.query.by;
			else me.birth_year = '';
			if (typeof me.$route.query.dy != 'undefined') me.death_year = me.$route.query.dy;
			else me.death_year = '';
			if (typeof me.$route.query.require_catalogs != 'undefined') {
				me.require_catalogs = me.$route.query.require_catalogs.replace(/[^0-9,]/, '').split(/,/);
				me.require_catalogs_string = me.require_catalogs.join(',');
				me.min_catalogs_required = 1; // Default
			}
			if (typeof me.$route.query.min_catalogs_required != 'undefined') {
				me.min_catalogs_required = me.$route.query.min_catalogs_required * 1;
			}
			if (me.edits_todo.length > 0) {
				mnm_notify("Edits still running!", 'warning');
				return;
			}
			if (typeof me.$route.query.prop != 'undefined') me.prop = me.$route.query.prop;
			me.loaded = false;
			me.loaded_wd = false;
			var params = {
				mode: (typeof me.mode == 'undefined' ? '' : me.mode),
				require_unset: 1,
				min: min,
				require_catalogs: me.require_catalogs.join(','),
				min_catalogs_required: me.min_catalogs_required
			};
			if (me.ext_name != '') params.ext_name = me.ext_name;
			if (me.birth_year != '') params.birth_year = me.birth_year;
			if (me.death_year != '') params.death_year = me.death_year;
			if (me.prop != '') params.prop = me.prop;
			try {
				var d = await mnm_api('creation_candidates', params);
				Object.entries(d.data.entries).forEach(function ([k, v]) {
					// Ensure reactive keys exist so Vue wires setters (Rust micro-API omits null fields)
					if (typeof v.q === 'undefined') v.q = null;
					if (typeof v.user === 'undefined') v.user = null;
					if (typeof v.username === 'undefined') v.username = null;
					if (typeof v.timestamp === 'undefined') v.timestamp = null;
					if (typeof d.data.users[v.user] == 'undefined') return;
					d.data.entries[k].username = d.data.users[v.user].name;
				});
				// Ensure all referenced catalogs are cached
				var catalog_ids = [...new Set(Object.values(d.data.entries).map(function (e) { return e.catalog; }))];
				await ensure_catalogs(catalog_ids);
				me.entries = d.data.entries;
				me.groupEntries();
				me.loadDataWikidata();
				var _ncc = document.querySelector('.next_cc_set'); if (_ncc) _ncc.focus();
			} catch (e) {
				mnm_notify("No results, parameters might be too restrictive", 'warning');
			}
			me.loaded = true;
			if (me.ext_name != '') {
				mnm_fetch_json('https://commons.wikimedia.org/w/api.php', {
					action: 'query',
					list: "search",
					srsearch: '"' + me.ext_name + '"',
					srnamespace: 6,
					format: 'json',
					origin: '*'
				}).then(function (d) {
					if (d.query.searchinfo.totalhits > 0) me.has_commons_search_results = true;
				});
			}
		},
		reloadWithParameters: function () {
			const me = this;
			var path = '/creation_candidates/';
			if (typeof me.mode != 'undefined' && me.mode != '') path += me.mode + '/';
			if (me.require_catalogs_string != '') {
				path += "?require_catalogs=" + me.require_catalogs_string;
				if (me.min_catalogs_required * 1 == 0) me.min_catalogs_required = 1; // No point in requiring catalogs if you don't require a single one of them
				path += "&min_catalogs_required=" + me.min_catalogs_required;
				me.require_catalogs = me.require_catalogs_string.replace(/[^0-9,]/, '').split(/,/);
			} else {
				me.require_catalogs = [];
				me.min_catalogs_required = 0;
			}
			me.$router.push(path);
			me.loadData();
		},
		hasHumanEntries: function () {
			return this.entries.some(function (e) { return e.type === 'Q5'; });
		},
		groupEntries: function () {
			const me = this;
			me.can_group_by_birth_year = false;
			var is_human = me.hasHumanEntries();

			if (is_human) {
				// Count distinct birth years to decide if grouping is possible
				var by_year = {};
				me.entries.forEach(function (entry) {
					var year = (entry.born || '').replace(/-.*$/, '');
					var key = year || '_no_year';
					if (typeof by_year[key] == 'undefined') by_year[key] = [];
					by_year[key].push(entry);
				});
				var keys = Object.keys(by_year);
				me.can_group_by_birth_year = keys.length > 1;

				if (me.group_by_birth_year && keys.length > 1) {
					keys.sort(function (a, b) {
						if (a === '_no_year') return 1;
						if (b === '_no_year') return -1;
						return (a * 1) - (b * 1);
					});
					me.grouped_entries = keys.map(function (k) {
						return { label: k === '_no_year' ? 'Unknown birth year' : 'Born ' + k, entries: by_year[k] };
					});
					return;
				}
			}

			me.grouped_entries = [{ label: null, entries: me.entries }];
		},
		toggleBirthYearGrouping: function () {
			this.group_by_birth_year = !this.group_by_birth_year;
			this.groupEntries();
		},
		filteredName: function () {
			var ret = this.entries[0].ext_name;
			ret = ret.replace(/^(Sir|Madam|Madame|Saint) /, '');
			ret = ret.replace(/\s*\(.+?\)\s*/, ' ');
			ret = ret.replace(/\s*\b[A-Z]\.\s*/, ' ');
			return ret;
		},
		loadDataWikidata: async function () {
			const me = this;
			me.loaded_wd = false;
			me.wd_entries = [];
			if (me.entries.length == 0) return; // Paranoia
			try {
				var d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
					action: 'wbsearchentities',
					search: me.filteredName(),
					language: 'en',
					limit: 20,
					type: 'item',
					format: 'json',
					origin: '*'
				});
				(d.search || []).forEach(function (v) {
					if (v.repository != 'local' && v.repository != 'wikidata') return;
					me.wd_entries.push(v);
				});
			} finally {
				me.loaded_wd = true;
			}
		},
		toggleCheckboxes: function (event) {
			var card = event.target.closest('div.card');
			if (card) {
				card.querySelectorAll('input.entry-list-item-checkbox').forEach(function (el) { el.click(); });
				this.checkbox_generation++;
			}
		},
		onCheckboxChange: function () {
			this.checkbox_generation++;
		},
		checkedInGroup: function (groupIdx) {
			// Access checkbox_generation to make Vue re-evaluate when checkboxes change
			void this.checkbox_generation;
			var card = this.$el ? this.$el.querySelectorAll('div.cc-group')[groupIdx] : null;
			if (!card) return 0;
			return card.querySelectorAll('input.entry-list-item-checkbox:checked').length;
		},
		getCheckedEntries: function (groupIdx) {
			var card = this.$el ? this.$el.querySelectorAll('div.cc-group')[groupIdx] : null;
			if (!card) return [];
			var checkedIds = {};
			card.querySelectorAll('input.entry-list-item-checkbox:checked').forEach(function (el) {
				checkedIds[el.getAttribute('entry')] = true;
			});
			var result = [];
			this.entries.forEach(function (e) { if (checkedIds[e.id]) result.push(e); });
			return result;
		},
		showAssignQ: function (groupIdx) {
			var me = this;
			// Pre-fill with Q if all fully matched entries in the group share the same one
			var prefill = '';
			var group_entries = (me.grouped_entries[groupIdx] || {}).entries || [];
			var matched_q = null;
			var consistent = true;
			group_entries.forEach(function (e) {
				if (e.q != null && e.q > 0 && e.user > 0) {
					if (matched_q === null) matched_q = e.q;
					else if (matched_q != e.q) consistent = false;
				}
			});
			if (matched_q !== null && consistent) prefill = 'Q' + matched_q;
			Vue.set(me.assign_q_input, groupIdx, prefill);
			me.$nextTick(function () {
				var input = me.$el.querySelectorAll('div.cc-group')[groupIdx];
				if (input) input = input.querySelector('input.assign-q-field');
				if (input) input.focus();
			});
		},
		cancelAssignQ: function (groupIdx) {
			Vue.delete(this.assign_q_input, groupIdx);
		},
		assignQToChecked: async function (groupIdx) {
			var me = this;
			var q = (me.assign_q_input[groupIdx] || '').replace(/\D/g, '');
			if (!q) { mnm_notify('Please enter a Q-number', 'warning'); return; }
			var checked = me.getCheckedEntries(groupIdx);
			if (checked.length === 0) { mnm_notify('No entries selected', 'warning'); return; }
			Vue.set(me.assigning_q, groupIdx, true);
			var done = 0;
			for (var i = 0; i < checked.length; i++) {
				var entry = checked[i];
				await new Promise(function (resolve) {
					me.setEntryQ(entry, q, false, function () { done++; resolve(); }, function () { resolve(); }, { silent: true });
				});
			}
			Vue.delete(me.assigning_q, groupIdx);
			Vue.delete(me.assign_q_input, groupIdx);
			me.checkbox_generation++;
			if (done > 0) mnm_notify('Assigned Q' + q + ' to ' + done + ' ' + (done === 1 ? 'entry' : 'entries'), 'success');
		},
		resetDefaultEntry: function () {
			document.querySelectorAll('input.entry-list-item-default-entry').forEach(function (el) { el.checked = false; });
		},
		createNewItem: function () {
			const me = this;
			var p31 = '';
			if (me.mode == 'human') p31 = 'Q5';
			var born = '';
			var died = '';
			var other_statements = {};

			me.entries.forEach(function (e) {
				var el = document.querySelector('input.entry-list-item-checkbox[entry="' + e.id + '"]');
				var is_checked = el ? el.checked : false;
				if (!is_checked) return;
				//				if ( e.wd_prop == null || e.wd_qual != null ) return ;
				if (p31 == '' && e.type != '') p31 = e.type;
				if (born.length < ('' + (e.born || '')).length) born = e.born;
				if (died.length < ('' + (e.died || '')).length) died = e.died;
				if (/\bfemale\b/.test(e.ext_desc)) other_statements.P21 = 'Q6581072';
				if (/\bmale\b/.test(e.ext_desc)) other_statements.P21 = 'Q6581097';
				if (/\b(taxon|species|genus)\b/.test(e.ext_desc)) p31 = 'Q16521';
				if (/\b(species)\b/.test(e.ext_desc)) other_statements.P105 = 'Q7432';
				if (/\b(author|writer)\b/.test(e.ext_desc)) other_statements.P106 = 'Q36180';
				if (/\bpainter\b/.test(e.ext_desc)) other_statements.P106 = 'Q1028181';
				me.edits_todo.push(e);
				var row = document.querySelector('div.entry_row[entry="' + e.id + '"]');
				if (row) row.classList.add('inactive');
				//				console.log ( e.has_person_date ) ;
			});

			if (p31 != 'Q5') delete other_statements.P106;
			if (p31 != '') other_statements.P31 = p31;

			if (me.edits_todo.length == 0) {
				mnm_notify("Please select at least one entry to base the new item on", 'warning');
				return;
			}

			// TODO use born,died,p31

			var checkedRadio = document.querySelector('input[name="default_entry"]:checked');
			let default_entry = checkedRadio ? checkedRadio.value : 0;
			me.newItemForEntry(me.edits_todo, me.doNextEdit, other_statements, default_entry, { silent: true });
			me.resetDefaultEntry();
		},
		doNextEdit: function () {
			const me = this;
			var doneRow = document.querySelector('div.entry_row[entry="' + me.edits_todo[0].id + '"]');
			if (doneRow) doneRow.classList.remove('inactive');
			me.edits_todo.shift();
			if (me.edits_todo.length == 0) return; // All done
			var q = ('' + (me.last_created_q || '')).replace(/\D/g, '');
			if (!q || q === '0') {
				// Creation failed — abort without assigning Q0 to remaining entries
				me.edits_todo.forEach(function (e) {
					var row = document.querySelector('div.entry_row[entry="' + e.id + '"]');
					if (row) row.classList.remove('inactive');
				});
				me.edits_todo = [];
				return;
			}
			me.setEntryQ(me.edits_todo[0], me.last_created_q, false, me.doNextEdit, undefined, { silent: true });
		}
	},
	watch: {
		'$route'(to, from) {
			console.log("!");
			this.loadData();
		}
	},
	template: `
	<div>
		<mnm-breadcrumb :crumbs="[{tt: 'creation_candidates'}]"></mnm-breadcrumb>
		<h2>
			<div v-if="entries.length>0" style='float:right'>
				<button class="btn btn-outline-primary" @click.prevent='entries=[];loadData();return false'
					tt='next_set'></button>
			</div>
			<span tt='creation_candidates'></span>
		</h2>
		<div v-if="!loaded"><i tt='loading'></i></div>
		<div v-else-if="entries.length==0" class="text-muted py-3" tt="no_results"></div>
		<div v-else>
			<div v-if="can_group_by_birth_year" class="mb-2">
				<label style="cursor:pointer;font-weight:normal">
					<input type="checkbox" :checked="group_by_birth_year" @change="toggleBirthYearGrouping" />
					Group by birth year
				</label>
			</div>
			<div v-for='(group,num) in grouped_entries' :key='num' class='cc-group' style='margin-bottom:20px'>
				<div v-if="group.label" class="text-muted fw-bold small border-bottom mb-1 pb-1">{{group.label}} ({{group.entries.length}})</div>
				<div class='card'>
					<div style="display:table;width:100%" @change="onCheckboxChange">
						<entry-list-item v-for="e in group.entries" :entry="e" :show_catalog="1" :show_permalink="1"
							:twoline="1" :key="e.id" :show_checkbox="1"
							:radio_name='mode=="by_ext_name"?null:"default_entry"'></entry-list-item>
					</div>
					<div class="card-footer py-1 d-flex flex-wrap align-items-center gap-1">
						<button class="btn btn-outline-secondary mnm-action-btn" @click.prevent="toggleCheckboxes"
							tt="toggle_checkboxes"></button>
						<button v-if="mode != 'by_ext_name'" class="btn btn-outline-secondary mnm-action-btn"
							@click.prevent="resetDefaultEntry" tt="reset_default_entry"></button>
						<template v-if="widar.is_logged_in && checkedInGroup(num) > 0">
							<button v-if="typeof assign_q_input[num] === 'undefined'"
								class="btn btn-outline-primary mnm-action-btn"
								@click.prevent="showAssignQ(num)">
								Assign Q to {{checkedInGroup(num)}} checked</button>
							<span v-else class="d-inline-flex align-items-center gap-1">
								<input type="text" class="form-control form-control-sm assign-q-field" style="width:8em"
									v-model="assign_q_input[num]" placeholder="Q12345"
									@keyup.enter="assignQToChecked(num)" @keyup.esc="cancelAssignQ(num)" />
								<button class="btn btn-primary mnm-action-btn"
									:disabled="!!assigning_q[num]"
									@click.prevent="assignQToChecked(num)">{{assigning_q[num] ? 'Assigning\u2026' : 'Go'}}</button>
								<button class="btn btn-outline-secondary mnm-action-btn"
									@click.prevent="cancelAssignQ(num)">&times;</button>
							</span>
						</template>
					</div>
				</div>
			</div>

			<div>
				<button class="btn btn-outline-primary next_cc_set" @click.prevent='entries=[];loadData();return false'
					tt='next_set'></button>
				<span v-if='widar.is_logged_in'>
					<button v-if='edits_todo.length==0' class='btn btn-outline-success' @click.prevent='createNewItem'
						tt_title='creation_warning'><span tt='create_new_item_for'></span>
						"{{entries[0].ext_name}}"</button>
				</span>
				<a :href='"https://commons.wikimedia.org/w/index.php?title=Special:MediaSearch&type=image&search="+entries[0].ext_name'
					target='_blank' class='btn btn-outline-success'>
					<span tt='search_commons'></span>
					<span v-if='has_commons_search_results'>&nbsp;✓</span>
				</a>
				<a :href='"https://www.google.com/search?q="+entries[0].ext_name+ "&tbm=isch&hl=en-US&tbs=il:cl"'
					target='_blank' class='btn btn-outline-success' tt='search_google_images_cc'></a>
			</div>
		</div>

		<div style='margin-top:20px;'> <!-- Wikidata search results -->
			<div class="card" style="margin-bottom:1em">
				<div class="card-body">
					<h4 class="card-title"><span tt='wikidata_search_results'></span><span v-if='loaded_wd'>
							"{{entries[0].ext_name}}"</span></h4>
					<div class="card-text">
						<div v-if="loaded_wd">
							<div v-if="wd_entries.length>0" class="results_overflow_box">
								<table class="table table-sm table-striped">
									<tbody>
										<tr v-for="e in wd_entries">
											<td nowrap>
												<a class='wikidata' target='_blank' :href='e.url'>{{e.id}}</a>
												<br /><small>{{e.id}}</small>
											</td>
											<td style='width:100%'>
												<div><b>{{e.label}}</b></div>
												<div style='font-size:10pt;-family:serif;'>
													<wd-desc autodesc_first='1' :item='e.id.replace(/\\D/g,"")'
														autodesc_fallback='1'></wd-desc>
												</div>
											</td>
										</tr>
									</tbody>
								</table>
							</div>
							<div v-else tt='no_matches'></div>
						</div>
						<div v-else tt='loading'></div>
					</div>
				</div>
			</div>
		</div>

	</div>
`
});
