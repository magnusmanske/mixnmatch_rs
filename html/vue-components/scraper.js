import { mnm_api, mnm_fetch_json, mnm_notify, tt_update_interface, widar } from './store.js';

// Shared inline reference for the $n / $Ln / $RID notation used in both
// the Scraper and Resolve cards.
const DOLLAR_REF_HELP =
	"Use $1, $2, … for capture groups from the entry regex, $L1, $L2, … for the current value of each level, and $RID for the fully resolved ID (the id field after its own regex replacements). $RID is available in every resolve field except id itself.";

const scraperTemplate = `<div>
		<mnm-breadcrumb :crumbs="[{text: 'Scraper'}]"></mnm-breadcrumb>

		<!-- ── Intro + quick-action toolbar ── -->
		<div class='d-flex justify-content-between align-items-start flex-wrap gap-2 mb-3'>
			<div>
				<h2 class='mb-1'>Scraper wizard</h2>
				<p class='text-muted mb-0' style='max-width:56rem'>
					Build an automated scraper for a web-based catalog. The scraper generates
					a list of URLs, fetches each page, and extracts Mix'n'match entries using
					regular expressions.
				</p>
			</div>
			<div class='d-flex gap-2 align-items-start mt-1'>
				<button class='btn btn-sm btn-outline-info' @click.prevent='loadExample'>Load example</button>
			</div>
		</div>


		<!-- ── 1. Catalog metadata ── -->
		<div class='card mb-3'>
			<div class='card-body'>
				<h4 class='card-title mb-1'>Catalog</h4>
				<div class='text-muted mb-3' style='font-size:0.9em'>
					Add a scraper to an existing catalog (enter its ID) or create a new one
					(leave the ID blank).
					<br />Only the original creator of an existing catalog can overwrite its
					scraper; anyone can add a new catalog. Entering a Wikidata property and
					leaving the field auto-fills name, description and URL.
				</div>

				<form>
					<!-- Catalog ID + Load existing settings -->
					<div class='row g-3 align-items-end mb-3'>
						<div class='col-md-5' style='max-width:24rem'>
							<label class='form-label'>Catalog ID</label>
							<div class='input-group'>
								<span class='input-group-text'>#</span>
								<input type='number' class='form-control' v-model='meta.catalog_id' placeholder='leave empty to create new'>
								<button v-if='meta.catalog_id' class='btn btn-outline-secondary' type='button' @click.prevent='loadExistingSettings'>Load settings</button>
							</div>
							<div class='form-text' v-if='!meta.catalog_id'>Leave empty to create a new catalog; when set, the metadata below is ignored on save.</div>
						</div>
						<div class='col-md' v-if='load_settings_status'>
							<div :class='"alert py-2 mb-0 " + (load_settings_status=="error" ? "alert-warning" : "alert-success")' style='font-size:0.9em'>
								{{load_settings_message}}
							</div>
						</div>
					</div>

					<div class='mb-3'>
						<label class='form-label'>Catalog name</label>
						<input type='text' class='form-control' v-model='meta.name'>
					</div>

					<div class='mb-3'>
						<label class='form-label'>Description</label>
						<input type='text' class='form-control' v-model='meta.desc'>
					</div>

					<div class='mb-3'>
						<label class='form-label'>URL <small class='text-muted'>(optional)</small></label>
						<input type='text' class='form-control' v-model='meta.url'>
					</div>

					<div class='row g-3'>
						<div class='col-md-4' style='max-width:18rem'>
							<label class='form-label'>Wikidata property <small class='text-muted'>(optional)</small></label>
							<div class='input-group'>
								<span class='input-group-text'>P</span>
								<input type='text' class='form-control' v-model='meta.property' @blur='onPropertyChanged' placeholder='e.g. 7471'>
							</div>
							<div class='form-text text-warning' v-if='property_already_used'>
								&#x26A0; Property already used by {{property_used_by}}
							</div>
						</div>
						<div class='col-md-4'>
							<label class='form-label' tt='type'></label>
							<select class='form-select' v-model='meta.type'>
								<option v-for='group in types' :value='group' :selected='group==meta.type'>{{ucFirst(group)}}</option>
							</select>
						</div>
						<div class='col-md-3' style='max-width:12rem'>
							<label class='form-label'>Primary language</label>
							<input type='text' class='form-control' v-model='meta.lang' placeholder='en'>
						</div>
					</div>
				</form>
			</div>
		</div>


		<!-- ── 2. Levels ── -->
		<div class='card mb-3'>
			<div class='card-body'>
				<div class='d-flex justify-content-between align-items-start flex-wrap gap-2 mb-2'>
					<div>
						<h4 class='card-title mb-1'>Levels</h4>
						<div class='text-muted' style='font-size:0.9em;max-width:56rem'>
							A URL is built from a static part plus one or more variables ("levels").
							Each level is a list of keys (e.g. letters), a numeric range, a page to
							follow, or a MediaWiki instance. The last level advances first; when it
							finishes, the one above it ticks forward and the last level restarts.
						</div>
					</div>
					<div class='dropdown flex-shrink-0'>
						<button type='button' class='btn btn-outline-success dropdown-toggle'
							data-bs-toggle='dropdown' aria-expanded='false'>+ Add level</button>
						<ul class='dropdown-menu dropdown-menu-end'>
							<li><a class='dropdown-item' href='#' @click.prevent='addLevel("keys")'>Keys <small class='text-muted'>(A–Z, custom list)</small></a></li>
							<li><a class='dropdown-item' href='#' @click.prevent='addLevel("range")'>Range <small class='text-muted'>(numeric from/to)</small></a></li>
							<li><a class='dropdown-item' href='#' @click.prevent='addLevel("follow")'>Follow <small class='text-muted'>(links on a page)</small></a></li>
							<li><a class='dropdown-item' href='#' @click.prevent='addLevel("mediawiki")'>MediaWiki <small class='text-muted'>(article namespace)</small></a></li>
						</ul>
					</div>
				</div>

				<div v-if='levels.length==0' class='alert alert-warning mt-2 mb-0' style='font-size:0.9em'>
					At least one level is required. Use the <strong>+ Add level</strong> button above to begin.
				</div>

				<div v-for='(l,level_id) in levels' class='border rounded p-3 mt-3' style='background:rgba(0,0,0,.02)'>
					<div class='d-flex justify-content-between align-items-center mb-3'>
						<div>
							<span class='badge text-bg-secondary me-2'>Level {{level_id+1}}</span>
							<span class='badge text-bg-light text-dark border' style='text-transform:uppercase;letter-spacing:.05em'>{{l.mode}}</span>
						</div>
						<div class='btn-group btn-group-sm'>
							<button type='button' class='btn btn-outline-secondary' v-if='level_id>0' @click.prevent='moveLevelUp(level_id)' title='Move up'>&uarr;</button>
							<button type='button' class='btn btn-outline-secondary' v-if='level_id+1<levels.length' @click.prevent='moveLevelDown(level_id)' title='Move down'>&darr;</button>
							<button type='button' class='btn btn-outline-danger' @click.prevent='deleteLevel(level_id)'>Delete</button>
						</div>
					</div>

					<div v-if='l.mode=="keys"' class='col'>
						<div class='mb-2 d-flex gap-2 align-items-center' style='font-size:0.9em'>
							<span class='text-muted'>Quick-fill:</span>
							<button type='button' class='btn btn-sm btn-outline-secondary' @click.prevent='setUcAZ'>A–Z</button>
							<button type='button' class='btn btn-sm btn-outline-secondary' @click.prevent='setLcAZ'>a–z</button>
						</div>
						<textarea class='form-control font-monospace' rows='5' @keyup='keysChanged' :level='level_id'
							placeholder='One key per line (e.g. A, B, C, …)'>{{l.keys.join("\\n")}}</textarea>
					</div>

					<div v-if='l.mode=="range"' class='col'>
						<div class='row g-3'>
							<div class='col-sm-3'>
								<label class='form-label'>Start</label>
								<input type='number' class='form-control' v-model='l.start'>
							</div>
							<div class='col-sm-3'>
								<label class='form-label'>End</label>
								<input type='number' class='form-control' v-model='l.end'>
							</div>
							<div class='col-sm-3'>
								<label class='form-label'>Step</label>
								<input type='number' class='form-control' v-model='l.step'>
							</div>
							<div class='col-sm-3'>
								<label class='form-label'>Zero-pad to</label>
								<input type='number' class='form-control' v-model='l.pad' min='0'>
								<div class='form-text'>E.g. 5 turns 42 into 00042. Use 0 for no padding.</div>
							</div>
						</div>
					</div>

					<div v-if='l.mode=="follow"' class='col'>
						<div class='mb-3'>
							<label class='form-label'>URL</label>
							<input type='text' class='form-control font-monospace' v-model='l.url'>
							<div class='form-text'>A URL pattern with <code>\$1</code> as a placeholder for a partial URL match from the regex below.</div>
						</div>
						<div class='mb-0'>
							<label class='form-label'>Regex</label>
							<input type='text' class='form-control font-monospace' v-model='l.rx'>
							<div class='form-text'>Regular expression matching (partial) URLs on a page built from lower levels — the first capture group feeds <code>\$1</code>.</div>
						</div>
					</div>

					<div v-if='l.mode=="mediawiki"' class='col'>
						<div class='mb-0'>
							<label class='form-label'>API URL</label>
							<input type='text' class='form-control font-monospace' v-model='l.url'>
							<div class='form-text'>URL of the MediaWiki API. Scrapes every article-namespace entry; <code>\$1</code> receives the article title.</div>
						</div>
					</div>
				</div>

				<div v-if='levels.length>0' class='alert alert-info mt-3 mb-0' style='font-size:0.9em'>
					{{getURLestimate()}}
				</div>
			</div>
		</div>


		<!-- ── Remaining cards only make sense once at least one level exists ── -->
		<div v-if='levels.length>0'>

			<!-- ── 3. Scraper ── -->
			<div class='card mb-3'>
				<div class='card-body'>
					<h4 class='card-title mb-1'>Scraper</h4>
					<div class='text-muted mb-3' style='font-size:0.9em'>
						Build the URL from the level values, then extract entries from the fetched
						HTML with regular expressions.
					</div>
					<form>
						<div class='mb-3'>
							<label class='form-label'>URL pattern</label>
							<input type='text' class='form-control font-monospace' v-model='scraper.url'>
							<div class='form-text'>Use <code>\$1</code> for the value of level 1, <code>\$2</code> for level 2, …</div>
						</div>
						<div class='mb-3'>
							<label class='form-label'>Block regex <small class='text-muted'>(optional)</small></label>
							<input type='text' class='form-control font-monospace' v-model='scraper.rx_block'>
							<div class='form-text'>Regex wrapping a block of entries; its <code>\$1</code> feeds into the entry regex. Useful for large/complex pages.</div>
							<div class='text-info' v-if='test_results.status=="OK" && scraper.rx_block!=""' style='font-size:0.9em'>
								Matches <strong>{{block_matches}}</strong> block(s) in the test HTML below.
							</div>
						</div>
						<div class='mb-0'>
							<label class='form-label'>Entry regex</label>
							<input type='text' class='form-control font-monospace' v-model='scraper.rx_entry'>
							<div class='form-text'>Regex matching a single entry. Capture groups <code>\$1</code>, <code>\$2</code>, … are referenced in <em>Resolve</em> below.</div>
							<div class='text-info' v-if='test_results.status=="OK" && scraper.rx_entry!=""' style='font-size:0.9em'>
								Matches <strong>{{internal_results.length}}</strong> entries in the test HTML below.
							</div>
						</div>
					</form>
				</div>
			</div>


			<!-- ── 4. Resolve ── -->
			<div class='card mb-3'>
				<div class='card-body'>
					<h4 class='card-title mb-1'>Resolve</h4>
					<div class='text-muted mb-3' style='font-size:0.9em'>${DOLLAR_REF_HELP}</div>
					<div v-for='(r,rid) in scraper.resolve' class='border-bottom pb-3 mb-3'>
						<form>
							<div class='row g-3 align-items-center'>
								<div class='col-md-2'>
									<label class='form-label fw-semibold text-uppercase mb-0' style='letter-spacing:.05em'>{{rid}}</label>
								</div>
								<div class='col-md-10'>
									<input type='text' class='form-control font-monospace' v-model='r.use'>
									<div class='form-text' v-if='note[rid]'>{{note[rid]}}</div>
								</div>
							</div>
							<div v-if='typeof r.rx!="undefined"' class='row g-3 mt-1'>
								<div class='col-md-2'></div>
								<div class='col-md-10'>
									<div class='text-muted mb-2' style='font-size:0.85em'>
										Optional regex replacements: <em>match &rarr; replace with</em>.
									</div>
									<div v-for='(rx,rxid) in r.rx' class='row g-2 mb-2 align-items-center'>
										<div class='col-auto' style='font-family:monospace;color:#888'>{{rxid+1}}.</div>
										<div class='col-sm-5'><input type='text' class='form-control form-control-sm font-monospace' v-model='rx[0]' placeholder='pattern' /></div>
										<div class='col-sm-5'><input type='text' class='form-control form-control-sm font-monospace' v-model='rx[1]' placeholder='replacement' /></div>
										<div class='col-sm-auto'>
											<button class='btn btn-sm btn-outline-danger' @click.prevent='removeRegex' :rid='rid' :rxid='rxid'>Delete</button>
										</div>
									</div>
									<button class='btn btn-sm btn-outline-success' @click.prevent='addRegex' :rid='rid'>+ Add regex replacement</button>
								</div>
							</div>
						</form>
					</div>
				</div>
			</div>


			<!-- ── 5. Options ── -->
			<div class='card mb-3'>
				<div class='card-body'>
					<h4 class='card-title mb-3'>Options</h4>
					<div class='form-check mb-2'>
						<input class='form-check-input' type='checkbox' id='opt_simple_space' v-model='options.simple_space'>
						<label class='form-check-label' for='opt_simple_space'>
							Compress whitespace (spaces, tabs, newlines) to a single space before processing
							<small class='text-muted'>— recommended, simplifies regex</small>
						</label>
					</div>
					<div class='form-check mb-2'>
						<input class='form-check-input' type='checkbox' id='opt_utf8_encode' v-model='options.utf8_encode'>
						<label class='form-check-label' for='opt_utf8_encode'>
							UTF-8 encode <small class='text-muted'>— usually not needed</small>
						</label>
					</div>
					<div class='form-check mb-0'>
						<input class='form-check-input' type='checkbox' id='opt_skip_failed' v-model='options.skip_failed'>
						<label class='form-check-label' for='opt_skip_failed'>
							Ignore pages that fail to load (403, timeouts, …) instead of aborting
							<small class='text-muted'>— good for slow/unreliable servers</small>
						</label>
					</div>
				</div>
			</div>


			<!-- ── 6. Test & save ── -->
			<div class='card mb-4'>
				<div class='card-body'>
					<h4 class='card-title mb-1'>Test &amp; save</h4>
					<div class='text-muted mb-3' style='font-size:0.9em'>
						Run a test to fetch the first URL and preview the entries your regex finds.
						Saving is only enabled once a test returns at least one entry.
					</div>

					<div class='d-flex gap-2 flex-wrap align-items-center mb-3'>
						<button class='btn btn-outline-success' @click.prevent='testScraper' :disabled='test_results.status=="RUNNING"'>
							<span v-if='test_results.status=="RUNNING"'>
								<span class='spinner-border spinner-border-sm me-1' role='status' aria-hidden='true'></span>
								Testing…
							</span>
							<span v-else>Test this scraper</span>
						</button>
						<button id='save_scraper_button' class='btn btn-primary'
							:disabled='can_save_message!="OK"'
							@click.prevent='saveScraper'>Save scraper / catalog</button>
						<span v-if='can_save_message && can_save_message!="OK"' class='text-muted' style='font-size:0.9em'>
							&mdash; {{can_save_message}}
						</span>
					</div>

					<div v-if='catalog_from_save!=0' class='alert alert-success'>
						Scraper saved. Monitor progress at
						<a :href='"https://mix-n-match.toolforge.org/#/catalog/"+catalog_from_save'>catalog #{{catalog_from_save}}</a>;
						the first run may take minutes to hours (rarely days).
					</div>

					<div v-if='test_results.status!="" && test_results.status!="OK" && test_results.status!="RUNNING"'
						class='alert alert-danger'>
						<strong>Test failed:</strong> {{test_results.status}}
					</div>

					<!--
						Diagnostics panel. Shown whenever the backend returned
						a diagnostics blob (most useful when a test reports
						zero results or fails to fetch). Surfaces the pieces
						of info the server knows but the user can't see: HTTP
						status, per-regex match counts, options as applied,
						and any warnings the backend computed.
					-->
					<div v-if='test_results.diagnostics && (test_results.status=="OK" || test_results.status=="")'>
						<div v-for='(w,i) in (test_results.diagnostics.warnings||[])' :key='"w-"+i'
							class='alert alert-warning py-2 mb-2' style='font-size:0.9em'>
							&#x26A0; {{w}}
						</div>
					</div>

					<details v-if='test_results.diagnostics'
						class='card mb-3' open
						style='border-color:rgba(0,0,0,.1)'>
						<summary class='card-body py-2' style='cursor:pointer;list-style:revert;font-weight:600'>
							Diagnostics
						</summary>
						<div class='card-body pt-0' style='font-size:0.9em'>
							<!-- HTTP row -->
							<div class='row g-2 mb-2' v-if='test_results.diagnostics.http'>
								<div class='col-sm-3 text-muted'>HTTP</div>
								<div class='col-sm-9'>
									<span v-if='test_results.diagnostics.http.error' class='text-danger'>
										{{test_results.diagnostics.http.error}}
									</span>
									<span v-else>
										<span :class='"badge me-2 " + (test_results.diagnostics.http.status>=200 && test_results.diagnostics.http.status<300 ? "text-bg-success" : "text-bg-danger")'>
											{{test_results.diagnostics.http.status}}
										</span>
										<span v-if='test_results.diagnostics.http.content_type' class='text-muted me-2'>
											{{test_results.diagnostics.http.content_type}}
										</span>
										<span class='text-muted'>
											{{test_results.diagnostics.http.body_length}} bytes
										</span>
									</span>
								</div>
							</div>

							<!-- HTML length / whitespace compression -->
							<div class='row g-2 mb-2' v-if='typeof test_results.diagnostics.html_length_before_compression!="undefined"'>
								<div class='col-sm-3 text-muted'>HTML size</div>
								<div class='col-sm-9'>
									{{test_results.diagnostics.html_length_before_compression}} chars
									<span v-if='test_results.diagnostics.html_length_after_compression != test_results.diagnostics.html_length_before_compression'
										class='text-muted'>
										&rarr; {{test_results.diagnostics.html_length_after_compression}} after whitespace compression
									</span>
								</div>
							</div>

							<!-- Options as applied -->
							<div class='row g-2 mb-2' v-if='test_results.diagnostics.options'>
								<div class='col-sm-3 text-muted'>Options in effect</div>
								<div class='col-sm-9'>
									<span v-for='(v,k) in test_results.diagnostics.options' :key='"opt-"+k'
										class='badge me-1'
										:class='v ? "text-bg-secondary" : "text-bg-light text-dark border"'>
										{{k}}: {{v ? "on" : "off"}}
									</span>
								</div>
							</div>

							<!-- Block regex -->
							<div class='row g-2 mb-2' v-if='test_results.diagnostics.regex && test_results.diagnostics.regex.block'>
								<div class='col-sm-3 text-muted'>Block regex</div>
								<div class='col-sm-9'>
									<div>
										<span class='badge text-bg-primary me-2'>
											{{test_results.diagnostics.regex.block.match_count}} match(es)
										</span>
									</div>
									<code class='d-block mt-1 text-break' style='font-size:0.85em;color:#555'>
										{{test_results.diagnostics.regex.block.source}}
									</code>
								</div>
							</div>

							<!-- Entry regex(es) -->
							<div class='row g-2 mb-2' v-if='test_results.diagnostics.regex && test_results.diagnostics.regex.entries && test_results.diagnostics.regex.entries.length'>
								<div class='col-sm-3 text-muted'>Entry regex</div>
								<div class='col-sm-9'>
									<div v-for='(e,idx) in test_results.diagnostics.regex.entries' :key='"e-"+idx' class='mb-2'>
										<span v-if='test_results.diagnostics.regex.entries.length>1' class='text-muted me-1'>#{{idx+1}}</span>
										<span :class='"badge me-2 " + (e.match_count>0 ? "text-bg-primary" : "text-bg-light text-dark border")'>
											{{e.match_count}} match(es)
										</span>
										<code class='d-block text-break' style='font-size:0.85em;color:#555'>
											{{e.source}}
										</code>
									</div>
								</div>
							</div>
						</div>
					</details>

					<div v-if='test_results.status=="OK"'>
						<div v-if='typeof test_results.last_url!="undefined"' class='mb-2'>
							<span class='text-muted me-1' style='font-size:0.9em'>URL fetched:</span>
							<a :href='test_results.last_url' class='external' target='_blank'>{{test_results.last_url}}</a>
						</div>
						<details class='mb-3'>
							<summary class='text-muted' style='font-size:0.9em;cursor:pointer'>Raw HTML response ({{(test_results.html||'').length}} chars)</summary>
							<textarea style='width:100%;font-family:monospace;font-size:8pt;' rows='5' readonly>{{test_results.html}}</textarea>
						</details>
						<div class='mb-2'>
							<span class='badge text-bg-primary'>{{test_results.results.length}} entries found</span>
						</div>
						<div v-if='test_results.results.length>0' class='table-responsive'>
							<table class='table table-sm table-striped align-middle'>
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
											<span v-else><a :href='r.url' target='_blank' class='external'>{{r.id}}</a></span>
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
		// Wire-format options: {key: 1} for truthy, drop falsy. The server
		// accepts bool/number/string now, but keep this normalisation so
		// the test, save, and stored shapes stay identical (load re-reads
		// the same shape, so round-trips stay byte-identical).
		normalizedOptions: function () {
			var out = {};
			Object.entries(this.options).forEach(function (e) { if (e[1]) out[e[0]] = 1; });
			return out;
		},
		saveScraper: async function () {
			const me = this;
			if (!me.canSaveScraper()) return;
			var params = { scraper: me.scraper, options: me.normalizedOptions(), levels: me.levels, tusc_user: widar.getUserName(), meta: me.meta };
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
		generateJSON: function () {
			// Options must hit the wire in the same normalised shape save
			// uses — previously this emitted JS booleans, which the
			// server's options parser silently treated as all-false
			// (it was calling as_u64() on a bool). Bug surfaced as a
			// test run ignoring the "Compress whitespace" checkbox.
			return JSON.stringify({ levels: this.levels, options: this.normalizedOptions(), scraper: this.scraper }, null, 2);
		}
	},
	watch: {
		'$route'(to, from) { this.init(to.params.opt); },
		scraper: { deep: true, handler: function () { this.calculateMatches(); } },
		options: { deep: true, handler: function () { this.calculateMatches(); } },
	},
	template: scraperTemplate
});
