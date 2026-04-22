import { mnm_api, mnm_fetch_json, mnm_notify, tt_update_interface, widar } from './store.js';

const scraperTemplate = `<div>
		<mnm-breadcrumb :crumbs="[{text: 'Scraper'}]"></mnm-breadcrumb>

		<p>This page helps to create an automated web page scraper, to generate and update Mix'n'match catalogs.<br />
			The goal is to create a list of URLs, iterate through them, and scrape the respective pages to generate
			Mix'n'match entries.<br />
			<span>[<a href='#' @click.prevent='loadExample()'>See example</a>]</span>
		</p>

		<div class="card mb-2">
			<div class="card-body">
				<h4 class="card-title">Catalog</h4>
				<h6 class="card-subtitle mb-2 text-muted">Add a scraper to an existing catalog (give ID), or create a
					new catalog (leave ID empty).<br />
					<i>Note: only the original catalog creator can save to an existing catalog, but everyone can add a
						new one.</i><br />
					<i>Note: if you enter the property first and then click another field, some information will be
						filled in automatically.</i>
				</h6>
				<div class="card-text">

					<form>

						<div class="mb-3 row align-items-center">
							<label class="col-sm-2 col-form-label">Catalog ID</label>
							<div class="col-sm-2"><input type="number" class="form-control" v-model="meta.catalog_id">
							</div>
							<div class="col-sm">
								<button v-if='meta.catalog_id' class='btn btn-sm btn-outline-secondary me-2' @click.prevent='loadExistingSettings'>Load existing settings</button>
								<span v-if='load_settings_status' :class='load_settings_status=="error"?"text-danger":"text-success"'>{{load_settings_message}}</span>
								<small v-if='!meta.catalog_id'>Leave empty to create a new catalog. If given, other data in this section will be ignored.</small>
							</div>
						</div>
						<div class="mb-3 row align-items-center">
							<label class="col-sm-2 col-form-label">Catalog name</label>
							<div class="col-sm-10"><input type="text" class="form-control" v-model="meta.name"></div>
						</div>
						<div class="mb-3 row align-items-center">
							<label class="col-sm-2 col-form-label">Description</label>
							<div class="col-sm-10"><input type="text" class="form-control" v-model="meta.desc"></div>
						</div>
						<div class="mb-3 row align-items-center">
							<label class="col-sm-2 col-form-label">URL</label>
							<div class="col-sm-8"><input type="text" class="form-control" v-model="meta.url"></div>
							<div class="col-sm-2"><small>optional</small></div>
						</div>
						<div class="mb-3 row align-items-center">
							<label class="col-sm-2 col-form-label">WD property</label>
							<div class="col-sm-2"><input type="text" class="form-control" v-model="meta.property"
									@blur="onPropertyChanged"></div>
							<div class="col-sm-2"><small>optional</small></div>
							<div class="col-sm-2" tt='type'></div>
							<div class="col-sm-2">
								<select class="form-select" v-model='meta.type'>
									<option v-for='group in types' :value='group' :selected='group==meta.type'>
										{{ucFirst(group)}}</option>
								</select>
							</div>
							<div class="col-sm text-warning" v-if='property_already_used'>&#x26A0; Property already used by {{property_used_by}}</div>
						</div>
						<div class="mb-3 row align-items-center">
							<label class="col-sm-2 col-form-label">Primary language</label>
							<div class="col-sm-2"><input type="text" class="form-control" v-model="meta.lang"></div>
						</div>


					</form>

				</div>
			</div>
		</div>


		<div class="card mb-2">
			<div class="card-body">
				<h4 class="card-title">Levels</h4>
				<div class="card-text">
					<p>A URL can be constructed from a static part, and one or more variables, here called
						<i>levels</i>. Each level can be a defined list of keys (e.g., letters), a range (numeric
						from-to, plus step size),
						or follow (get URLs listed on a page and follow them). The last level with be run through,
						before the level above it (next lower level) ticks ahead, and the higher level resets.<br />
						So, if the first level is keys A-Z, and the second is range 1-100 (step size 1), URLs will use
						A/1, A/2,... A/100, B/1, B/2,... Z/100.
					</p>
					<div>
						<div v-for='(l,level_id) in levels' class='row'>
							<div class='col-2' style='text-align:right;font-family:Lato,Arial,Courier'>Level
								{{level_id+1}}<br />{{l.mode}}</div>
							<div v-if='l.mode=="keys"' class='col'>
								<div><textarea style='width:100%' rows='5' @keyup='keysChanged' :level='level_id'
										placeholder='Keys (e.g., A-Z, one per row)'>{{l.keys.join("\\n")}}</textarea>
								</div>
								<div>
									Set keys to:
									<a href='#' @click.prevent='setUcAZ'>A-Z</a> |
									<a href='#' @click.prevent='setLcAZ'>a-z</a>
								</div>
							</div>
							<div v-if='l.mode=="range"' class='col'>
								<form>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">Start</label>
										<div class="col-sm-10"><input type="number" class="form-control"
												v-model="l.start"></div>
									</div>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">End</label>
										<div class="col-sm-10"><input type="number" class="form-control"
												v-model="l.end"></div>
									</div>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">Step</label>
										<div class="col-sm-10"><input type="number" class="form-control"
												v-model="l.step"></div>
									</div>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">Zero-pad to</label>
										<div class="col-sm-10 input-with-note"><input type="number" class="form-control"
												v-model="l.pad" min="0"><small>Minimum number of digits (e.g. 5 turns 42 into 00042). Leave 0 for no padding.</small></div>
									</div>
								</form>
							</div>
							<div v-if='l.mode=="follow"' class='col'>
								<form>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">URL</label>
										<div class="col-sm-10 input-with-note"><input type="text" class="form-control"
												v-model="l.url"><small>A URL pattern with \$1 as a placeholder for a
												partial URL match from the RegEx</small></div>
									</div>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">Regex</label>
										<div class="col-sm-10 input-with-note"><input type="text" class="form-control"
												v-model="l.rx"><small>Regular expression matching (partial) URLs on a
												page from the lower levels, as first parameter</small></div>
									</div>
								</form>
							</div>
							<div v-if='l.mode=="mediawiki"' class='col'>
								<form>
									<div class="mb-3 row">
										<label class="col-sm-2 col-form-label">URL</label>
										<div class="col-sm-10 input-with-note"><input type="text" class="form-control"
												v-model="l.url"><small>The URL of the API of the MediaWiki installation.
												Will scrape all entries in article namespace. \$1 will have the article
												title.</small></div>
									</div>
								</form>
							</div>
							<div class='col-4'>
								<button class='btn btn-outline-danger' @click='deleteLevel(level_id)'>Delete
									level</button>
								<button v-if='level_id>0' class='btn btn-light'
									@click='moveLevelUp(level_id)'>&uArr;</button>
								<button v-if='level_id+1<levels.length' class='btn btn-light'
									@click='moveLevelDown(level_id)'>&dArr;</button>
							</div>
						</div>
					</div>
					<div>
						<button class='btn btn-outline-success' @click='addLevel("keys")'>Add keys level</button>
						<button class='btn btn-outline-success' @click='addLevel("range")'>Add range level</button>
						<button class='btn btn-outline-success' @click='addLevel("follow")'>Add follow level</button>
						<button class='btn btn-outline-success' @click='addLevel("mediawiki")'>Add MediaWiki
							level</button>
					</div>
					<div>{{getURLestimate()}}</div>
					<div v-if='levels.length==0' class='text-danger'>At least one level is required</div>
				</div>
			</div>
		</div>


		<div v-if='levels.length>0'>
			<div class="card mb-2">
				<div class="card-body">
					<h4 class="card-title">Scraper</h4>
					<h6 class="card-subtitle mb-2 text-muted">Now use the level values to construct the URLs to be
						scraped, then define block/entry-level regular expressions to get the data</h6>
					<div class='card-text'>
						<form>
							<div class="mb-3 row">
								<label class="col-sm-2 col-form-label">URL pattern</label>
								<div class="col-sm-10 input-with-note"><input type="text" class="form-control"
										v-model="scraper.url"><small>A URL pattern, with \$1 for the value of level 1, \$2
										for level 2 etc.</small></div>
							</div>
							<div class="mb-3 row">
								<label class="col-sm-2 col-form-label">RegEx block</label>
								<div class="col-sm-10 input-with-note">
									<input type="text" class="form-control" v-model="scraper.rx_block">
									<small>Regular expression for blocks of entries, useful for scraping large pages;
										every part \$1 will be used for the entry RegEx below. Optional</small>
									<div class='text-info' v-if='test_results.status=="OK" && scraper.rx_block!=""'>This
										regular expression matches {{block_matches}} blocks in the HTML below</div>
								</div>
							</div>
							<div class="mb-3 row">
								<label class="col-sm-2 col-form-label">RegEx entry</label>
								<div class="col-sm-10 input-with-note">
									<input type="text" class="form-control" v-model="scraper.rx_entry">
									<small>Regular expression to match one single entry. You can use the matching parts
										() as \$1, \$2,... to resolve the Mix'n'match values</small>
									<div class='text-info' v-if='test_results.status=="OK" && scraper.rx_entry!=""'>This
										regular expression matches {{internal_results.length}} entries in the HTML below
									</div>
								</div>
							</div>
						</form>
					</div>
				</div>
			</div>


			<div class="card mb-2">
				<div class="card-body">
					<h4 class="card-title">Resolve</h4>
					<h6 class="card-subtitle mb-2 text-muted">Use \$1, \$2,... for the parts of RegEx entry, and \$L1,
						\$L2,... for the current values of the levels</h6>
					<div class='card-text'>
						<div v-for='(r,rid) in scraper.resolve'>
							<form>
								<div class="mb-3">
									<div class="row">
										<label class="col-sm-2 col-form-label">{{rid}}</label>
										<div class="col-sm-10 input-with-note"><input type="text" class="form-control"
												v-model="r.use"><small>{{note[rid]}}</small></div>
									</div>
									<div v-if='typeof r.rx!="undefined"' class='row'>
										<div class='col-sm-2'></div>
										<div class='col-sm-10'>
											<div><small class='text-muted'>Optional: Use one or more regular expressions
													to "fix up" values; match => replace with</small></div>
											<div v-for='(rx,rxid) in r.rx' class='row align-items-center'>
												<div class='col-sm-1' style='text-align:right;font-family:Courier;'>
													{{rxid+1}}</div>
												<div class='col-sm-4'><input type='text' class='form-control'
														v-model='rx[0]' placeholder='The matching pattern' /></div>
												<div class='col-sm-4'><input type='text' class='form-control'
														v-model='rx[1]' placeholder='The replacement' /></div>
												<div class='col-sm-2'>
													<button class='btn btn-outline-danger' @click.prevent='removeRegex'
														:rid='rid' :rxid='rxid'>Delete</button>
												</div>
											</div>
											<div><button class='btn btn-outline-success' @click.prevent='addRegex'
													:rid='rid'>Add regular expression replacement</button></div>
										</div>
									</div>
								</div>
							</form>
						</div>
					</div>
				</div>
			</div>

			<div class="card mb-2">
				<div class="card-body">
					<h4 class="card-title">Options</h4>
					<div class="card-text">
						<div class="form-check"><label class="form-check-label"><input class="form-check-input"
									type="checkbox" v-model="options.simple_space">Compress whitespace (spaces, tabs,
								newlines) to single space before processing <i>(recommended, makes for easier
									regex)</i></label></div>
						<div class="form-check"><label class="form-check-label"><input class="form-check-input"
									type="checkbox" v-model="options.utf8_encode">UTF8-encode <i>(usually not
									needed)</i></label></div>
						<div class="form-check"><label class="form-check-label"><input class="form-check-input"
									type="checkbox" v-model="options.skip_failed">Ignore pages that fail to load (403, timeouts, etc.) instead of aborting <i>(good for slow/problematic servers)</i></label></div>
					</div>
				</div>
			</div>


			<div class="card mb-2">
				<div class="card-body">
					<h4 class="card-title">Testing and saving</h4>
					<div class="card-text">
						<div class='mb-2'>
							<button class='btn btn-outline-success' @click.prevent='testScraper'>Test this
								scraper</button>
							<span v-if='can_save_message=="OK"'><button id='save_scraper_button'
									class='btn btn-outline-primary' @click.prevent='saveScraper'>Save
									scraper/catalog</button></span>
							<span v-else>{{can_save_message}}</span>
						</div>

						<div v-if='test_results.status==""'></div>
						<div v-else-if='test_results.status=="RUNNING"'><i>Test is running...</i></div>
						<div v-else-if='test_results.status=="OK"'>

							<div v-if='catalog_from_save!=0'>
								The scraper for <a
									:href='"https://mix-n-match.toolforge.org/#/catalog/"+catalog_from_save'>this
									catalog</a> was successfully saved, run can take minutes/hours (days in rare cases).
							</div>

							<div>
								<h6 class="card-title">Test results</h6>
								<div v-if='typeof test_results.last_url!="undefined"'>
									<h7>URL used</h7>
									<a :href='test_results.last_url' class='external'
										target='_blank'>{{test_results.last_url}}</a>
								</div>
								<div>
									<h7>HTML of page</h7>
									<textarea style='width:100%;font-family:Courier;font-size:8pt;' rows=5
										readonly>{{test_results.html}}</textarea>
								</div>
								<div>
									<h7>Scraped entries</h7>
									<div>{{test_results.results.length}} entries found on page</div>
									<div v-if='test_results.results.length>0'>
										<table class='table table-sm table-sm table-striped'>
											<thead>
												<tr>
													<th>ID</th>
													<th>Name</th>
													<th>Description</th>
													<th>Type</th>
												</tr>
											</thead>
											<tbody>
												<tr v-for='(r,rid) in test_results.results'>
													<td>
														<span v-if='r.url==""'>{{r.id}}</span>
														<span v-else><a :href='r.url' target='_blank'
																class='external'>{{r.id}}</a></span>
													</td>
													<td>{{r.name}}</td>
													<td>{{r.desc}}</td>
													<td>{{r.type}}</td>
												</tr>
											</tbody>
										</table>
									</div>
								</div>
							</div>


						</div>
						<div v-else style='color:red'>ERROR: {{test_results.status}}</div>
					</div>
				</div>
			</div>

			<!--
<div>
<h4>JSON [debugging info]</h4>
<textarea rows='10' style='width:100%'>{{generateJSON()}}</textarea>
</div>
</div>
-->

		</div>
</div>`;

export default Vue.extend({
	props: ["opt"],
	data: function () {
		return {
			types: [], type2count: {}, levels: [], options: { simple_space: false, utf8_encode: false, skip_failed: true }, is_example: false, test_results: { status: '' }, scraper: {
				url: '', rx_block: '', rx_entry: '', resolve: {
					id: { 'use': '', rx: [] },
					name: { 'use': '', rx: [] },
					desc: { 'use': '', rx: [] },
					url: { 'use': '', rx: [] },
					type: { 'use': '', rx: [] },
				}
			}, meta: { catalog_id: '', name: '', desc: '', property: '', lang: 'en', type: 'unknown', url: '' }, note: {
				id: '',
				name: 'For people (Q5), try to get "first_name last_name", maybe with a RegEx (below): ^(.+?), (.+)$ => $2 $1',
				desc: 'For people (Q5), try to get birth/death dates into the description, it can help with the auto-matching',
				url: 'E.g. https://www.thesource.com/entry/$1 if $1 is the entry ID',
				type: 'Use a Q number to set a default type; optional (e.g. Q5=human; Q16521=taxon)',
			}, block_matches: 0, internal_results: [], can_save_message: '', catalog_from_save: 0, property_already_used: false, property_used_by: '',
			load_settings_status: '', load_settings_message: ''
		};
	},
	created: function () {
		const me = this;
		me.init('');
		Object.keys(me.meta).forEach(function (k) {
			if (typeof me.$route.query[k] == 'undefined') return;
			me.meta[k] = me.$route.query[k];
		});
		// Deep-link from the catalog Action menu: /scraper/new/:catalog_id
		// pre-fills the id and auto-loads the existing scraper settings so
		// the user lands on a populated form rather than an empty one.
		var routeCid = (me.$route.params && me.$route.params.catalog_id) || '';
		if (routeCid) {
			me.meta.catalog_id = routeCid;
			me.loadExistingSettings();
		}
		if (me.meta['property'] != '') me.onPropertyChanged();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		init: function (o) { this.updateTypes(); },
		loadExistingSettings: async function () {
			const me = this;
			me.load_settings_status = '';
			me.load_settings_message = '';
			if (!me.meta.catalog_id) {
				me.load_settings_status = 'error';
				me.load_settings_message = 'Enter a catalog ID first.';
				return;
			}
			try {
				// get_scraper is a dedicated endpoint that returns the
				// stored scraper/options/levels already parsed, plus a
				// freshly-reconstructed meta block from the catalog row.
				// The previous implementation piggybacked on
				// catalog_overview (which returns an array) and indexed
				// it as a map — so it never found anything.
				var d = await mnm_api('get_scraper', { catalog: me.meta.catalog_id });
				var j = d.data || {};
				if (!j.found) {
					me.load_settings_status = 'error';
					me.load_settings_message = 'No autoscrape settings found for this catalog.';
					return;
				}
				if (j.scraper) me.scraper = j.scraper;
				if (Array.isArray(j.levels)) me.levels = j.levels;
				// Options come back as a plain object; fill every key of
				// the form model (truthy → checked, missing → unchecked).
				if (j.options) {
					Object.keys(me.options).forEach(function (k) { me.options[k] = !!j.options[k]; });
				}
				// Meta is rebuilt server-side from the catalog row so the
				// form's name/desc/property/lang/type/url fields reflect
				// current state. Preserve the catalog_id we asked for so
				// toggling between "load" / "save" doesn't drop it.
				if (j.meta) {
					Object.keys(me.meta).forEach(function (k) {
						if (typeof j.meta[k] != 'undefined') me.meta[k] = j.meta[k];
					});
				}
				me.load_settings_status = 'ok';
				me.load_settings_message = 'Settings loaded.';
				if (me.meta.property != '') me.onPropertyChanged();
			} catch (e) {
				me.load_settings_status = 'error';
				me.load_settings_message = 'Failed to load settings: ' + (e.message || e);
			}
		},
		ucFirst: function (s) { return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, ' '); },
		updateTypes: async function () {
			const me = this;
			me.types = []; me.type2count = {};
			var d = await mnm_api('catalog_type_counts');
			(d.data || []).forEach(function (v) { me.type2count[v.type] = v.cnt; me.types.push(v.type); });
			me.types.sort();
		},
		loadExample: function () {
			const me = this;
			me.levels = [{ mode: 'keys', keys: "abcdefghijklmnopqrstuvwxyzõäöü".split('') }];
			me.test_results = { status: '' };
			me.scraper = { url: 'https://digikogu.ekm.ee/ekm/authors?letter=$1', rx_block: '', rx_entry: '<a href="/ekm/authors/author_id-(\\d+)">(.+?)</a>', resolve: { id: { 'use': '$1', rx: [] }, name: { 'use': '$2', rx: [['^(.+?), (.+)$', '$2 $1']] }, desc: { 'use': '', rx: [] }, url: { 'use': 'https://digikogu.ekm.ee/ekm/authors/author_id-$1', rx: [] }, type: { 'use': 'Q5', rx: [] } } };
			me.meta = { catalog_id: '', name: 'Art Museum of Estonia artist', desc: 'Artist identifier for the Art Museum of Estonia', property: 'P4563', lang: 'et', url: 'https://digikogu.ekm.ee', type: 'biography' };
			me.options = { simple_space: true, utf8_encode: false, skip_failed: false };
			me.is_example = true;
			me.canSaveScraper();
		},
		saveScraper: async function () {
			const me = this;
			if (!me.canSaveScraper()) return;
			var params = { scraper: me.scraper, options: {}, levels: me.levels, tusc_user: widar.getUserName(), meta: me.meta };
			Object.entries(me.options).forEach(function (e) { if (e[1]) params.options[e[0]] = 1; });
			params.scraper = JSON.stringify(params.scraper); params.meta = JSON.stringify(params.meta);
			params.levels = JSON.stringify(params.levels); params.options = JSON.stringify(params.options);
			var btn = document.getElementById('save_scraper_button');
			if (btn) btn.style.display = 'none';
			try {
				var d = await mnm_api('save_scraper', params, { method: 'POST' });
				me.catalog_from_save = d.data.catalog;
			} catch (e) { mnm_notify(e.message || 'Save failed', 'danger'); }
			finally { if (btn) btn.style.display = ''; }
		},
		canSaveScraper: function () {
			const me = this;
			me.can_save_message = '';
			if (me.is_example) { me.can_save_message = 'This is an example, you cannot save it'; return false; }
			if (me.test_results.status == '') { me.can_save_message = 'Run a test that returns results to save this scraper/catalog'; return false; }
			if (me.test_results.status == 'RUNNING') return false;
			if (me.test_results.status != 'OK') { me.can_save_message = 'ERROR: ' + me.test_results.status; return false; }
			if (me.test_results.results.length == 0) { me.can_save_message = 'Empty result set'; return false; }
			if (me.meta.catalog_id == '') {
				if (me.meta.name == '' || me.meta.desc == '' || me.meta.lang == '') { me.can_save_message = 'Catalog information missing'; return false; }
			}
			var username = widar.getUserName();
			if (typeof username == 'undefined') { me.can_save_message = 'You are not logged in'; return false; }
			me.can_save_message = 'OK';
			return true;
		},
		getURLestimate: function () {
			const me = this;
			if (me.levels.length == 0) return '';
			var product = 1; var is_uncertain = false;
			me.levels.forEach(function (l) { if (l.mode == 'keys') product *= l.keys.length; if (l.mode == 'range') product *= Math.floor((l.end - l.start + 1) / l.step); if (l.mode == 'follow') is_uncertain = true; });
			var ret = "This will scrape " + product + " URLs.";
			if (is_uncertain) ret += " Most likely a lot more, as there is no way to estimate the \"follow\" level type.";
			return ret;
		},
		doesCatalogWithPropertyExist: async function () {
			const me = this;
			var prop = (me.meta.property || '').replace(/\D/g, '');
			if (prop == '') return;
			// Exclude the catalog we're currently editing from the lookup —
			// otherwise "Load existing settings" for a catalog that owns
			// its own property instantly triggers the warning on itself.
			var params = { wd_prop: prop };
			var selfId = parseInt(me.meta.catalog_id, 10);
			if (selfId > 0) params.exclude_catalog = selfId;
			try {
				var d = await mnm_api('check_wd_prop_usage', params);
				me.property_already_used = d.data && d.data.used;
				me.property_used_by = me.property_already_used ? d.data.catalog_name + ' (#' + d.data.catalog_id + ')' : '';
			} catch (e) { /* ignore */ }
		},
		onPropertyChanged: function () {
			const me = this;
			me.doesCatalogWithPropertyExist();
			var prop = me.meta.property.replace(/\D/g, '');
			if (prop == '') return;
			prop = "P" + prop;
			mnm_fetch_json('https://www.wikidata.org/w/api.php', { action: 'wbgetentities', ids: prop, format: 'json', origin: '*' }).then(function (d) {
				var e = d.entities[prop];
				if (typeof e == 'undefined') return;
				if (me.meta.name == '' && typeof e.labels.en != 'undefined') me.meta.name = e.labels.en.value.replace(/ ID$/, '');
				if (me.meta.desc == '' && typeof e.descriptions.en != 'undefined') me.meta.desc = e.descriptions.en.value;
				if (typeof e.claims == 'undefined') return;
				if (me.meta.url == '' && typeof e.claims['P1896'] != 'undefined') me.meta.url = e.claims['P1896'][0].mainsnak.datavalue.value;
				if (me.scraper.resolve.url.use == '' && typeof e.claims['P1630'] != 'undefined') me.scraper.resolve.url.use = e.claims['P1630'][0].mainsnak.datavalue.value;
			});
		},
		calculateMatches: function () {
			const me = this;
			me.canSaveScraper();
			if (typeof me.test_results == 'undefined' || me.test_results.status != 'OK' || typeof me.test_results.html == 'undefined') return;
			me.block_matches = 0; me.internal_results = [];
			var h = me.test_results.html;
			if (me.options.simple_space) h = h.replace(/\s+/gm, ' ');
			var res_block = [];
			if (me.scraper.rx_block != '') {
				var r; try { r = new RegExp(me.scraper.rx_block, 'g'); } catch (e) { }
				if (typeof r != 'undefined') { var tmp; while ((tmp = r.exec(h)) !== null) { if (typeof tmp == 'undefined' || typeof tmp[1] == 'undefined') continue; res_block.push(tmp[1]); } me.block_matches = res_block.length; }
			} else res_block = [h];
			if (me.scraper.rx_entry != '') {
				var r2; try { r2 = new RegExp(me.scraper.rx_entry, 'g'); } catch (e) { }
				if (typeof r2 != 'undefined') { res_block.forEach(function (rh) { var tmp2; while ((tmp2 = r2.exec(rh)) !== null) { var o = []; for (var a = 1; a <= 9; a++) { if (typeof tmp2[a] != 'undefined') o[a] = tmp2[a]; } me.internal_results.push(o); } }); }
			}
			me.canSaveScraper();
		},
		testScraper: async function () {
			const me = this;
			me.test_results = { status: 'RUNNING' }; me.canSaveScraper();
			try {
				var d = await mnm_api('autoscrape_test', { json: me.generateJSON(), rand: Math.random() }, { method: 'POST' });
				me.test_results = d.data; me.test_results.status = d.status; me.calculateMatches();
			} catch (e) { me.test_results.status = e.message || 'Test has failed, for reasons unknown. Could be the server to be scraped is too slow?'; }
		},
		addLevel: function (mode) {
			const me = this;
			var o = { mode: mode };
			if (mode == 'keys') o.keys = [];
			else if (mode == 'range') { o.start = 0; o.end = 10; o.step = 1; }
			else if (mode == 'follow') { o.rx = ''; o.url = ''; }
			me.levels.push(o);
		},
		deleteLevel: function (level_id) { this.levels.splice(level_id, 1); },
		moveLevelUp: function (level_id) { this.swapLevels(level_id, level_id - 1); },
		moveLevelDown: function (level_id) { this.swapLevels(level_id, level_id + 1); },
		swapLevels: function (l1, l2) { var dummy = this.levels[l1]; Vue.set(this.levels, l1, this.levels[l2]); Vue.set(this.levels, l2, dummy); },
		addRegex: function (event) { var rid = event.target.getAttribute('rid'); this.scraper.resolve[rid].rx.push(['', '']); },
		removeRegex: function (event) { var rid = event.target.getAttribute('rid'); var rxid = event.target.getAttribute('rxid'); this.scraper.resolve[rid].rx.splice(rxid, 1); },
		keysChanged: function (event) { var level_id = event.target.getAttribute('level'); this.levels[level_id].keys = (event.target.value || '').trim().split("\n"); },
		setUcAZ: function (event) { var ta = event.target.closest('div.col').querySelector('textarea'); this.levels[ta.getAttribute('level')].keys = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split(''); },
		setLcAZ: function (event) { var ta = event.target.closest('div.col').querySelector('textarea'); this.levels[ta.getAttribute('level')].keys = "abcdefghijklmnopqrstuvwxyz".split(''); },
		generateJSON: function () { return JSON.stringify({ levels: this.levels, options: this.options, scraper: this.scraper }, null, 2); }
	},
	watch: {
		'$route'(to, from) { this.init(to.params.opt); },
		scraper: { deep: true, handler: function () { this.calculateMatches(); } },
		options: { deep: true, handler: function () { this.calculateMatches(); } },
	},
	template: scraperTemplate
});
