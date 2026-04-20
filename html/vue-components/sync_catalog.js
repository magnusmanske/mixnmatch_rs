import { mnm_api, mnm_fetch_json, mnm_loading, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

export default Vue.extend({
	props: ["id"],
	data: function () {
		return { catalog: {}, data: {}, entries: {}, loaded: false, load_error: '', mnm2wd: '', wd_duplicates: [], update_mnm_status: '', update_mnm_result: {} };
	},
	created: function () { this.loadData(); },
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface() },
	methods: {
		updateMNM: async function () {
			const me = this;
			me.update_mnm_status = 'updating';
			try {
				var d = await mnm_api('match_q_multi', {
					catalog: me.id,
					tusc_user: widar.getUserName(),
					data: JSON.stringify(me.data.wd_no_mm)
				}, { method: 'POST' });
				me.update_mnm_result = d;
				me.update_mnm_status = 'done';
				var el = document.getElementById('wd_no_mm');
				if (el) el.innerHTML = "<span tt='done'></span>";
			} catch (e) {
				me.update_mnm_status = 'failed';
			}
		},
		checkDoubleWD: async function () {
			const me = this;
			me.wd_duplicates = [];
			if (me.catalog.wd_prop == null) return;
			if (me.catalog.wd_qual != null) return;
			var sparql = 'SELECT ?entry (count(?q) AS ?cnt) (group_concat(?q) AS ?qs ) { ?q wdt:P' + me.catalog.wd_prop + ' ?entry } GROUP BY ?entry HAVING (?cnt>1)';
			var d = await mnm_fetch_json('https://query.wikidata.org/sparql', { format: 'json', query: sparql });
			var dupes = [];
			var ids = [];
			d.results.bindings.forEach(function (v) {
				var o = { ext_id: v.entry.value, qs: [] };
				o.qs = v.qs.value.match(/(Q\d+)\b/g);
				dupes.push(o);
				ids.push(o.ext_id);
			});
			if (ids.length > 0) {
				var d2 = await mnm_api('get_entry', { ext_ids: JSON.stringify(ids), catalog: me.catalog.id }, { method: 'POST' });
				Object.entries(d2.data.entries).forEach(function ([k, v]) {
					me.entries[k] = v;
					dupes.forEach(function (v1, k1) {
						if (v.ext_id == v1.ext_id) dupes[k1].id = v.id;
					});
					if (typeof d2.data.users[v.user] == 'undefined') return;
					d2.data.entries[k].username = d2.data.users[v.user].name;
				});
			}
			me.wd_duplicates = dupes;
		},
		loadData: async function () {
			const me = this;
			me.loaded = false;
			me.load_error = '';
			mnm_loading(true);
			try {
				await ensure_catalog(me.id);
				me.catalog = get_specific_catalog(me.id);
				if (!me.catalog || !me.catalog.wd_prop) {
					me.load_error = 'This catalog has no Wikidata property and cannot be synced.';
					me.loaded = true;
					mnm_loading(false);
					return;
				}
				me.entries = {};
				me.mnm2wd = '';

				var [syncResult] = await Promise.all([
					mnm_api('get_sync', { catalog: me.id }),
					me.checkDoubleWD().catch(function () { })
				]);

				me.data = syncResult.data;

				var mnm2wd = [];
				(me.data.mm_no_wd || []).some(function (v) {
					mnm2wd.push('Q' + v[0] + "\tP" + me.catalog.wd_prop + "\t" + '"' + decodeURIComponent(escape(v[1])) + '"');
					return mnm2wd.length > 20000;
				});
				if (mnm2wd.length > 0) me.mnm2wd = mnm2wd.join("\n");

				var ids = [];
				Object.values(me.data.mm_double || {}).forEach(function (v) {
					v.forEach(function (v1) { ids.push(v1) });
				});
				if (ids.length > 0) {
					var d2 = await mnm_api('get_entry', { entry: ids.join(',') }, { method: 'POST' });
					Object.entries(d2.data.entries).forEach(function ([k, v]) {
						me.entries[k] = v;
						if (typeof d2.data.users[v.user] == 'undefined') return;
						d2.data.entries[k].username = d2.data.users[v.user].name;
					});
				}

				me.loaded = true;
			} catch (e) {
				me.load_error = 'Sync failed: ' + e.message;
				me.loaded = true;
			}
			mnm_loading(false);
		}
	},
	template: `
	<div>
		<mnm-breadcrumb v-if='typeof catalog != "undefined" && catalog && catalog.id' :crumbs="[
			{text: catalog.name, to: '/catalog/'+catalog.id},
			{text: 'Sync'}
		]"></mnm-breadcrumb>
		<catalog-header v-if='typeof catalog != "undefined" && catalog && catalog.id' :catalog="catalog"></catalog-header>
		<div v-if='!loaded && !load_error' class='text-center py-4 mt-3'>
			<div class='spinner-border text-primary' role='status'></div>
			<p class='mt-2 text-muted'>Syncing with Wikidata&hellip; this may take a few minutes.</p>
		</div>
		<div v-if='load_error' class='alert alert-danger mt-3'>{{load_error}}</div>
		<div v-if='loaded && !load_error'>

			<div v-if='data.wd_no_mm.length==0 && mnm2wd.length==0 && wd_duplicates.length==0 && Object.keys(data.mm_double||{}).length==0' class='alert alert-success mt-3'>
				This catalog is fully in sync with Wikidata. No differences found.
			</div>

			<div v-if='data.wd_no_mm.length>0'>
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='wikidata_connections' :tt1='data.wd_no_mm.length'></h4>
						<div class="card-text">
							<div v-if='update_mnm_status==""'><button class='btn btn-outline-primary' tt='update_mnm'
									@click.prevent='updateMNM'></button></div>
							<div v-else-if='update_mnm_status=="updating"'><i tt='updating'></i></div>
							<div v-else>
								<div v-if='update_mnm_status=="done"' tt='done'></div>
								<div v-else>{{update_mnm_status}}</div>
								<div v-if='update_mnm_result.not_found>0'>
									{{update_mnm_result.not_found}} IDs from Wikidata not found in Mix'n'match.
									<div>External IDs not found in MnM (max 100):<br />
										<span v-for="id in update_mnm_result.not_found_list"
											style="margin-right: 1rem;">
											<a target="_blank" class="wikidata"
												:href="'https://www.wikidata.org/w/index.php?search=haswbstatement:%22P'+catalog.wd_prop+'='+encodeURIComponent(id)+'%22'">{{id}}</a>
										</span>
									</div>
								</div>
								<div v-if='(update_mnm_result.no_changes_written||[]).length>0'>
									<h5>{{update_mnm_result.no_changes_written.length}} mismatches between Wikidata and
										Mix'n'match</h5>
									<div v-for='d in update_mnm_result.no_changes_written'>
										<router-link :to='"/entry/"+d.entry.id'>Entry #{{d.entry.id}}</router-link>:
										Wikidata says <wd-link :item='d.new_q'></wd-link>, Mix'n'match says <wd-link
											:item='d.entry.q'></wd-link>
									</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>


			<div v-if='mnm2wd.length>0'>
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='connections_only_here' :tt1='data.mm_no_wd.length'></h4>
						<div class="card-text">
							<form action="//quickstatements.toolforge.org/api.php" method="post" target="_blank">
								<input type='hidden' name='action' value='import' />
								<input type='hidden' name='format' value='v1' />
								<input type='hidden' name='temporary' value='1' />
								<input type='hidden' name='openpage' value='1' />
								<input type="hidden" id="mm_no_wd_list" name="data" :value='mnm2wd' />
								<button class='btn btn-outline-primary' tt='update_wikidata' name='yup'></button>
							</form>
						</div>
					</div>
				</div>
			</div>

			<div v-if='wd_duplicates.length>0'>
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='double_on_wd'></h4>
						<div class="card-text">
							<table class='table table-sm'>
								<tbody>
									<tr v-for='x in wd_duplicates' v-if="typeof entries[x.id]!='undefined'">
										<td>
											<div style="display:table;width:100%;margin-bottom:10px"><entry-list-item
													:entry="entries[x.id]" :show_permalink="1"></entry-list-item></div>
											<div v-for='q in x.qs'>
												<wd-link :item='q.replace(/\\D/g,"")'></wd-link>
												<span style='font-size:10pt;-family:serif;'>
													<wd-desc :autodesc_first='entries[x.id].type=="Q5"'
														v-if='Object.keys(wd_duplicates).length<500'
														:item='q.replace(/\\D/g,"")' autodesc_fallback='1'></wd-desc>
													<span v-else><i tt='too_many_results_no_autodesc'></i></span>
												</span>
											</div>
										</td>
									</tr>
								</tbody>
							</table>
						</div>
					</div>
				</div>
			</div>

			<div v-if='Object.keys(data.mm_double||{}).length>0'>
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='double_q'></h4>
						<div class="card-text">
							<div v-for='(d,q) in data.mm_double' style='margin-bottom:1em'>
								<h5><wd-link :item='q' /></h5>
								<div style="display:table;width:100%">
									<span v-if='Object.keys(data.mm_double).length<500'>
										<entry-list-item v-for="e in d" v-if="entries[e]" :entry="entries[e]" :show_permalink="1"
											:key="e"></entry-list-item>
									</span>
									<span v-else>
										<span v-for="(e,idx) in d">
											<span v-if='idx>0'>; </span>
											<router-link :to='"/entry/"+e' :key='e'>Entry #{{e}}</router-link>
										</span>
										<i tt='too_many_results_no_autodesc'></i>
									</span>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>

		</div>
	</div>
`
});
