import { mnm_api, mnm_fetch_json, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

// Default-on booleans: present in kv_catalog with value "0" means opted out;
// absent/any-other-value means the default (on) applies.
const BOOL_KEYS_DEFAULT_ON = ['use_automatchers', 'use_description_for_new'];

// Default-off location toggles. All live in kv_catalog and only apply when
// the catalog also carries `has_locations=yes`.
const LOCATION_BOOL_KEYS = [
	'allow_location_operations',
	'allow_location_match',
	'location_allow_full_match',
	'allow_location_create',
	'location_force_same_type',
];

// Short inline help shown under each kv control.
const FIELD_HELP = {
	use_automatchers:          "Run the generic automatch jobs on this catalog (on by default).",
	use_description_for_new:   "Copy an entry's description into a newly-created Wikidata item (on by default).",
	automatch_sparql:          "SPARQL query that yields ?q and ?label. The \u2018automatch_sparql\u2019 job matches entries by label against the result set.",
	automatch_complex:         "Limits automatch to items reachable via (property, item) roots. Each row is one constraint, e.g. P31 \u2192 Q5 (\u201cinstance of human\u201d).",
	allow_location_operations: "Enable coordinate-based tools (quick compare / map) for this catalog.",
	allow_location_match:      "Let the coordinate matcher propose matches by location.",
	location_allow_full_match: "Permit fully automatic (no user review) location matches.",
	allow_location_create:     "Permit automatic item creation from unmatched location entries.",
	location_force_same_type:  "When matching by location, require the candidate item's P31 to match the entry's type.",
	location_distance:         "Maximum allowed distance (metres) between entry and candidate item.",
};

const SPARQL_PLACEHOLDER =
	"SELECT ?q ?label WHERE {\n" +
	"  ?q wdt:P31 wd:Q5 ;\n" +
	"     rdfs:label ?label .\n" +
	"  FILTER(LANG(?label) = \"en\")\n" +
	"}";

export default Vue.extend({
	props: ['id'],
	data: function () {
		return {
			catalog: {},
			loaded: false,
			saving: false,
			kv: {
				use_automatchers: true,
				use_description_for_new: true,
				automatch_sparql: '',
				automatch_complex: [],
				allow_location_operations: false,
				allow_location_match: false,
				location_allow_full_match: false,
				allow_location_create: false,
				location_force_same_type: false,
				location_distance: '',
			},
			label_cache: {},
			location_bool_keys: LOCATION_BOOL_KEYS,
			sparql_placeholder: SPARQL_PLACEHOLDER,
		};
	},
	created: async function () { await this.reload(); },
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	computed: {
		// Admin OR creator of this catalog can edit.
		can_edit: function () {
			if (widar.is_catalog_admin) return true;
			if (!widar.mnm_user_id || !this.catalog || !this.catalog.owner) return false;
			return widar.mnm_user_id == this.catalog.owner;
		},
		has_locations: function () {
			if (!this.catalog) return false;
			// `has_locations` lives in kv_catalog. It's exposed on the
			// overview as a loose top-level field for some pages, but the
			// authoritative source is kv_pairs — check both so the editor
			// works regardless of where the flag comes from.
			if (this.catalog.has_locations == 'yes') return true;
			const kv = this.catalog.kv_pairs || {};
			return kv.has_locations == 'yes';
		},
		// Qualifier-bearing catalogs are a legacy corner of MnM we're
		// deliberately not surfacing in this UI — show the property field
		// only when no qualifier is set.
		show_property_field: function () {
			return !(this.catalog && this.catalog.wd_qual);
		},
	},
	methods: {
		fieldHelp: function (key) { return FIELD_HELP[key] || ''; },
		reload: async function () {
			const me = this;
			me.loaded = false;
			await ensure_catalog(me.id, true);
			const c = get_specific_catalog(me.id);
			if (!c) { me.loaded = true; return; }
			// Shallow clone so edits don't leak into the shared cache until
			// the server acknowledges the save.
			me.catalog = Object.assign({}, c);
			me.catalog.active = !!(c.active | 0);
			me.hydrateKv(c.kv_pairs || {});
			me.loaded = true;
		},
		hydrateKv: function (pairs) {
			const me = this;
			BOOL_KEYS_DEFAULT_ON.forEach(function (k) {
				me.kv[k] = !(typeof pairs[k] != 'undefined' && pairs[k] == '0');
			});
			LOCATION_BOOL_KEYS.forEach(function (k) {
				me.kv[k] = typeof pairs[k] != 'undefined' && pairs[k] == '1';
			});
			me.kv.automatch_sparql = pairs.automatch_sparql || '';
			// Stored form is "200m"; strip the unit for display.
			me.kv.location_distance = (pairs.location_distance || '').replace(/m\s*$/i, '');
			me.kv.automatch_complex = me.parseAutomatchComplex(pairs.automatch_complex);
		},
		parseAutomatchComplex: function (raw) {
			if (!raw) return [];
			try {
				const arr = JSON.parse(raw);
				if (!Array.isArray(arr)) return [];
				return arr.filter(r => Array.isArray(r) && r.length >= 2)
					.map(r => [Number(r[0]) | 0, Number(r[1]) | 0]);
			} catch (e) { return []; }
		},
		addComplexRow: function () { this.kv.automatch_complex.push([0, 0]); },
		removeComplexRow: function (i) { this.kv.automatch_complex.splice(i, 1); },
		complexLabelFor: function (prefix, n) {
			if (!n) return '';
			const key = prefix + n;
			const cached = this.label_cache[key];
			if (typeof cached != 'undefined') return cached;
			// Mark pending so we don't fire the same wbgetentities request twice.
			this.$set(this.label_cache, key, '');
			this.fetchLabel(prefix, n);
			return '';
		},
		fetchLabel: async function (prefix, n) {
			const me = this;
			try {
				const ids = prefix + n;
				const d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
					action: 'wbgetentities', ids: ids, props: 'labels', languages: 'en',
					format: 'json', origin: '*'
				});
				const label = (((d.entities || {})[ids] || {}).labels || {}).en;
				me.$set(me.label_cache, ids, label ? label.value : '\u2014');
			} catch (e) { /* network hiccup — leave blank */ }
		},
		update_ext_urls: async function () {
			const me = this;
			const prop = 'P' + me.catalog.wd_prop;
			try {
				const d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
					action: 'wbgetentities', format: 'json', origin: '*', ids: prop
				});
				const x = (d.entities[prop] || {}).claims || {};
				if (typeof x.P1630 == 'undefined') { mnm_notify(prop + ' has no formatter URL', 'danger'); return; }
				let url = '';
				x.P1630.forEach(c => {
					if (c.rank == 'preferred' || (url == '' && c.rank == 'normal')) url = c.mainsnak.datavalue.value;
				});
				if (url == '') { mnm_notify(prop + ' has no suitable formatter URL', 'danger'); return; }
				await mnm_api('update_ext_urls', { username: widar.getUserName(), url: url, catalog: me.id });
				mnm_notify('Done', 'success');
			} catch (e) { mnm_notify(e.message, 'danger'); }
		},
		buildKvPayload: function () {
			const me = this;
			const out = {};
			// Default-on: unchecked → "0" (opt out), checked → "" (delete row = default).
			BOOL_KEYS_DEFAULT_ON.forEach(function (k) { out[k] = me.kv[k] ? '' : '0'; });
			// Default-off location toggles: checked → "1", unchecked → "" (delete).
			LOCATION_BOOL_KEYS.forEach(function (k) { out[k] = me.kv[k] ? '1' : ''; });
			out.automatch_sparql = (me.kv.automatch_sparql || '').trim();
			// Empty array → delete row; otherwise JSON-serialise the tuples.
			const rows = (me.kv.automatch_complex || [])
				.filter(r => Array.isArray(r) && (r[0] | 0) > 0 && (r[1] | 0) > 0)
				.map(r => [r[0] | 0, r[1] | 0]);
			out.automatch_complex = rows.length ? JSON.stringify(rows) : '';
			// Re-append the "m" suffix; blank/zero → delete.
			const dist = parseFloat(me.kv.location_distance);
			out.location_distance = isFinite(dist) && dist > 0 ? (dist + 'm') : '';
			return out;
		},
		onSave: async function () {
			const me = this;
			if (me.saving || !me.can_edit) return;
			me.saving = true;
			try {
				const payload = {
					name: me.catalog.name,
					desc: me.catalog.desc,
					url: me.catalog.url,
					type: me.catalog.type,
					search_wp: me.catalog.search_wp,
					wd_prop: me.catalog.wd_prop,
					wd_qual: me.catalog.wd_qual,
					active: !!me.catalog.active,
					kv: me.buildKvPayload(),
				};
				await mnm_api('edit_catalog', {
					username: widar.getUserName(),
					catalog: me.id,
					data: JSON.stringify(payload)
				}, { method: 'POST' });
				await ensure_catalog(me.id, true);
				mnm_notify('Catalog saved', 'success');
				router.push('/catalog/' + me.id);
			} catch (e) {
				mnm_notify('Save failed: ' + (e.message || e), 'danger');
			} finally { me.saving = false; }
		},
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{tt: 'catalog_editor'}
	]"></mnm-breadcrumb>
	<catalog-header :catalog="catalog"></catalog-header>

	<div v-if='!loaded'><i tt='loading'></i></div>

	<template v-else>
		<h2 class='mb-3' tt='catalog_editor'></h2>

		<div v-if='!can_edit' class='alert alert-warning'>
			You aren't a catalog admin or this catalog's creator, so you can't change settings here.
		</div>

		<form @submit.prevent='onSave'>
			<fieldset :disabled='!can_edit || saving'>

				<!-- ─── Basic metadata ─── -->
				<div class='card mb-3'>
					<div class='card-body'>
						<h5 class='card-title mb-3'>Basic information</h5>

						<div class='row g-3 mb-3'>
							<div class='col-md-8'>
								<label class='form-label'>Catalog name</label>
								<input type='text' class='form-control' v-model='catalog.name' />
							</div>
							<div class='col-md-4'>
								<label class='form-label'>Type</label>
								<input type='text' class='form-control' v-model='catalog.type' placeholder='e.g. person, place, work' />
							</div>
						</div>

						<div class='mb-3'>
							<label class='form-label'>Description</label>
							<input type='text' class='form-control' v-model='catalog.desc' />
						</div>

						<div class='row g-3 mb-3'>
							<div class='col-md-8'>
								<label class='form-label'>Catalog URL</label>
								<input type='text' class='form-control' v-model='catalog.url' />
							</div>
							<div class='col-md-4'>
								<label class='form-label'>Main language</label>
								<input type='text' class='form-control' v-model='catalog.search_wp' placeholder='en' />
							</div>
						</div>

						<div v-if='show_property_field' class='row g-3 align-items-end mb-3'>
							<div class='col-md-3' style='max-width:14rem'>
								<label class='form-label'>Wikidata property</label>
								<div class='input-group'>
									<span class='input-group-text'>P</span>
									<input type='number' min='0' class='form-control' v-model='catalog.wd_prop' />
								</div>
								<div class='form-text'>Numeric only. Leave blank if the catalog isn't linked to a Wikidata property.</div>
							</div>
							<div class='col-md' v-if='typeof catalog.wd_prop!="undefined" && catalog.wd_prop>0'>
								<a href='#' class='btn btn-outline-warning' @click.prevent='update_ext_urls'>
									Update external URLs from P{{catalog.wd_prop}} formatter URL
								</a>
								<div class='form-text text-danger'>Careful — rewrites every entry's ext_url.</div>
							</div>
						</div>

						<div class='form-check'>
							<input class='form-check-input' type='checkbox' id='cat-active' v-model='catalog.active' />
							<label class='form-check-label' for='cat-active'>Catalog is active</label>
						</div>
					</div>
				</div>

				<!-- ─── Automatchers & descriptions ─── -->
				<div class='card mb-3'>
					<div class='card-body'>
						<h5 class='card-title mb-3'>Automatchers</h5>

						<div class='form-check mb-3'>
							<input class='form-check-input' type='checkbox' id='kv-use-automatchers' v-model='kv.use_automatchers' />
							<label class='form-check-label' for='kv-use-automatchers'>Run generic automatchers on this catalog</label>
							<div class='form-text'>{{ fieldHelp('use_automatchers') }}</div>
						</div>

						<div class='form-check mb-3'>
							<input class='form-check-input' type='checkbox' id='kv-use-desc' v-model='kv.use_description_for_new' />
							<label class='form-check-label' for='kv-use-desc'>Use entry description when creating new Wikidata items</label>
							<div class='form-text'>{{ fieldHelp('use_description_for_new') }}</div>
						</div>

						<div class='mb-3'>
							<label class='form-label'>SPARQL query (for <code>automatch_sparql</code>)</label>
							<textarea class='form-control font-monospace' rows='5' v-model='kv.automatch_sparql'
								:placeholder='sparql_placeholder'></textarea>
							<div class='form-text'>{{ fieldHelp('automatch_sparql') }} Leave blank to remove the job.</div>
						</div>

						<div>
							<label class='form-label d-block'>Complex automatch constraints (<code>automatch_complex</code>)</label>
							<div class='form-text mb-2'>{{ fieldHelp('automatch_complex') }}</div>
							<table class='table align-middle' v-if='kv.automatch_complex.length'>
								<thead>
									<tr>
										<th style='width:12rem'>Property</th>
										<th style='width:12rem'>Item</th>
										<th style='width:100%'>Preview</th>
										<th style='width:3rem'></th>
									</tr>
								</thead>
								<tbody>
									<tr v-for='(row,i) in kv.automatch_complex' :key='i'>
										<td>
											<div class='input-group'>
												<span class='input-group-text'>P</span>
												<input type='number' min='1' class='form-control' v-model.number='row[0]' />
											</div>
										</td>
										<td>
											<div class='input-group'>
												<span class='input-group-text'>Q</span>
												<input type='number' min='1' class='form-control' v-model.number='row[1]' />
											</div>
										</td>
										<td>
											<small class='text-muted'>
												<a v-if='row[0]' target='_blank' :href='"https://www.wikidata.org/wiki/Property:P"+row[0]'>P{{row[0]}}</a>
												<span v-if='row[0]'> ({{ complexLabelFor('P', row[0]) || '\u2026' }})</span>
												<span v-if='row[0] && row[1]'> &rarr; </span>
												<a v-if='row[1]' target='_blank' :href='"https://www.wikidata.org/wiki/Q"+row[1]'>Q{{row[1]}}</a>
												<span v-if='row[1]'> ({{ complexLabelFor('Q', row[1]) || '\u2026' }})</span>
											</small>
										</td>
										<td>
											<button type='button' class='btn btn-sm btn-outline-danger' @click='removeComplexRow(i)'>&times;</button>
										</td>
									</tr>
								</tbody>
							</table>
							<button type='button' class='btn btn-sm btn-outline-primary' @click='addComplexRow'>+ Add constraint</button>
						</div>
					</div>
				</div>

				<!-- ─── Location settings (only if catalog advertises locations) ─── -->
				<div class='card mb-3' v-if='has_locations'>
					<div class='card-body'>
						<h5 class='card-title mb-3'>Location matching</h5>

						<div class='row g-3'>
							<div class='col-md-6' v-for='k in location_bool_keys' :key='k'>
								<div class='form-check'>
									<input class='form-check-input' type='checkbox' :id='"kv-"+k' v-model='kv[k]' />
									<label class='form-check-label' :for='"kv-"+k'>{{ k.replace(/_/g,' ') }}</label>
									<div class='form-text'>{{ fieldHelp(k) }}</div>
								</div>
							</div>
						</div>

						<div class='mt-3' style='max-width:16rem'>
							<label class='form-label'>Maximum distance</label>
							<div class='input-group'>
								<input type='number' min='0' step='1' class='form-control' v-model='kv.location_distance' />
								<span class='input-group-text'>metres</span>
							</div>
							<div class='form-text'>{{ fieldHelp('location_distance') }}</div>
						</div>
					</div>
				</div>

				<div v-if='can_edit' class='d-flex gap-2 mb-5'>
					<button type='submit' class='btn btn-primary' :disabled='saving'>
						<span v-if='saving'>Saving&hellip;</span>
						<span v-else>Save changes</span>
					</button>
					<router-link :to='"/catalog/"+catalog.id' class='btn btn-outline-secondary'>Cancel</router-link>
				</div>
			</fieldset>
		</form>
	</template>
</div>
`
});
