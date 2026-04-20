import { mnm_api, mnm_notify, mnm_loading, ensure_catalog, get_specific_catalog, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['id'],
	data: function () { return { catalog: {}, meta: {}, loaded: false, not_found: false, ext_url_pattern: '' } },
	created: async function () {
		await ensure_catalog(this.id);
		this.catalog = get_specific_catalog(this.id);
		if (typeof this.catalog == 'undefined') {
			this.not_found = true;
			this.loaded = true;
			return;
		}
		if (typeof this.catalog != 'undefined' && this.catalog.unmatched < 0) this.updateStats();
		else this.loadCatalog(this.id);
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface(); },
	computed: {
		is_active: function () {
			return this.catalog && this.catalog.active != null && this.catalog.active * 1 === 1;
		}
	},
	methods: {
		loadCatalog: function (id) {
			const me = this;
			me.loaded = false;
			me.not_found = false;
			if (typeof me.catalog == 'undefined') {
				me.not_found = true;
				me.loaded = true;
				return;
			}
			me.doLoadCatalog(id);
			if (typeof me.catalog.autoscrape_json != 'undefined') {
				let j = JSON.parse(me.catalog.autoscrape_json);
				me.ext_url_pattern = (((j.scraper || {}).resolve || {}).url || {}).use || '';
			}
		},
		doLoadCatalog: async function (id, _retries) {
			const me = this;
			_retries = _retries || 0;
			mnm_loading(true);
			try {
				var d = await mnm_api('catalog_details', { catalog: id });
				me.loaded = true;
				me.meta = d.data;
				await ensure_catalog(id, true);
				me.catalog = get_specific_catalog(id);
				if (typeof me.catalog == 'undefined') {
					me.not_found = true;
				}
			} catch (e) {
				if (_retries < 3) {
					mnm_loading(false);
					setTimeout(function () { me.doLoadCatalog(id, _retries + 1) }, 1000 * (_retries + 1));
					return;
				} else {
					me.not_found = true;
					me.loaded = true;
				}
			}
			mnm_loading(false);
		},
		updateStats: async function () {
			const me = this;
			await mnm_api('update_overview', { catalog: me.id });
			me.loadCatalog(me.id);
		}
	},
	watch: {
		'$route'(to, from) {
			this.loadCatalog(to.params.id);
		}
	},
	template: `
	<div> <!-- wrapper -->
	<mnm-breadcrumb v-if='typeof catalog != "undefined" && catalog && catalog.id' :crumbs="[
			{text: catalog.name}
		]"></mnm-breadcrumb>
		<mnm-breadcrumb v-else :crumbs="[
			{text: 'Catalog #'+id}
		]"></mnm-breadcrumb>

		<div v-if='not_found' class="alert alert-warning mt-3">
			Catalog #{{id}} was not found.
		</div>
		<div v-else-if='catalog && catalog.id && !is_active' class="alert alert-warning mt-3">
			<catalog-header :catalog="catalog" :nolink="1"></catalog-header>
			This catalog has been deactivated.
		</div>
		<div v-else-if='catalog && catalog.id'>
			<catalog-header :catalog="catalog" :nolink="1"></catalog-header>

			<p v-if='isNaN(catalog.total)'><b>
					This catalog appears to be empty, maybe the initial scraping is still running
				</b></p>

			<p>
				<span v-if='typeof catalog.username!="undefined"'><span tt='imported_by_user'></span>
					<a :href='"https://www.wikidata.org/wiki/User:"+encodeURIComponent(catalog.username)'
						target='_blank' class='wikidata'>{{catalog.username}}</a></span>
				<span v-if='typeof catalog.scrape_update!="undefined" && catalog.scrape_update'> | <span
						tt='updated_via_autoscrape'></span> <small>(<tt>{{catalog.last_scrape}}</tt>)</small></span>
				<span> | <a href="#" tt="update_stats" @click.prevent='updateStats'></a></span>
			</p>

			<div class="card mb-4">
				<div class="card-body">
					<h4 class="card-title" tt='entries'></h4>
					<div class="card-text">
						<table class='table table-sm table-striped'>
							<tbody>
								<tr>
									<td nowrap><router-link :to='"/list/"+catalog.id+"/manual"'
											tt="manually_matched"></router-link></td>
									<td class='num'>{{catalog.manual}}</td>
									<td style='width:100%'>
										<div class="progress">
											<div role="progressbar" class="progress-bar bg-success"
												:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(1000*catalog.manual/catalog.total)/10:0)+'%'}">
												{{catalog.total?(Math.floor(1000*catalog.manual/catalog.total)/10):0}}%
											</div>
										</div>
									</td>
								</tr>
								<tr>
									<td nowrap><router-link :to='"/list/"+catalog.id+"/auto"'
											tt="auto_matched"></router-link></td>
									<td class='num'>{{catalog.autoq}}</td>
									<td>
										<div class="progress">
											<div role="progressbar" class="progress-bar"
												:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(1000*catalog.autoq/catalog.total)/10:0)+'%'}">
												{{catalog.total?(Math.floor(1000*catalog.autoq/catalog.total)/10):0}}%
											</div>
										</div>
									</td>
								</tr>
								<tr>
									<td nowrap><router-link :to='"/list/"+catalog.id+"/na"'
											tt="not_applicable"></router-link></td>
									<td class='num'>{{catalog.na}}</td>
									<td>
										<div class="progress">
											<div role="progressbar" class="progress-bar bg-danger"
												:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(1000*catalog.na/catalog.total)/10:0)+'%'}">
												{{catalog.total?(Math.floor(1000*catalog.na/catalog.total)/10):0}}%
											</div>
										</div>
									</td>
								</tr>
								<tr>
									<td nowrap><router-link :to='"/list/"+catalog.id+"/unmatched"'
											tt="unmatched"></router-link></td>
									<td class='num'>{{catalog.unmatched}}</td>
									<td>
										<div class="progress">
											<div role="progressbar" class="progress-bar bg-info"
												:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(1000*catalog.unmatched/catalog.total)/10:0)+'%'}">
												{{catalog.total?(Math.floor(1000*catalog.unmatched/catalog.total)/10):0}}%
											</div>
										</div>
									</td>
								</tr>
								<tr>
									<td tt="total"></td>
									<td class='num'><b>{{catalog.total}}</b></td>
									<td class='num'></td>
								</tr>
							</tbody>
						</table>
					</div>
				</div>
			</div>

			<div v-if='loaded'>
				<div v-if="typeof meta.type != 'undefined'">

					<div class="card mb-4">
						<div class="card-body">
							<h4 class="card-title" tt='entry_types'></h4>
							<div class="card-text">
								<table class='table table-sm table-striped'>
									<tbody>
										<tr v-for="type in meta.type" v-bind:t="type">
											<th>
												<span v-if='type.type.match(/^Q\\d+$/)'><wd-link
														:item='type.type.replace(/^Q/,"")' :key='type.type' /></span>
												<span v-else>{{type.type}}</span>
											</th>
											<td class='num'>{{type.cnt}}</td>
										</tr>
									</tbody>
								</table>
							</div>
						</div>
					</div>

					<div class="card mb-4">
						<div class="card-body">
							<h4 class="card-title" tt='matches_over_time'></h4>
							<div class="card-text">
								<table class='table table-sm table-striped'>
									<tbody>
										<tr v-for="ym in meta.ym" v-bind:ym="ym">
											<th nowrap>{{ym.ym.substr(0,4)+'-'+ym.ym.substr(4,2)}}</th>
											<td style='width:100%'>
												<div class="progress" v-if="catalog.manual>0">
													<div role="progressbar" class="progress-bar bg-success"
														:style="{'white-space':'nowrap',width:Math.floor(100*ym.cnt/(catalog.manual+catalog.na))+'%'}">
														{{Math.floor(100*ym.cnt/(catalog.manual+catalog.na))}}%</div>
												</div>
											</td>
											<td nowrap class='num'>{{ym.cnt}}</td>
										</tr>
									</tbody>
								</table>
							</div>
						</div>
					</div>

					<div class="card mb-4">
						<div class="card-body">
							<h4 class="card-title" tt='users'></h4>
							<div class="card-text">
								<table class='table table-sm table-striped'>
									<tbody>
										<tr v-for="u in meta.user" v-bind:u="u">
											<td>
												<userlink :username='u.username' :user_id='u.uid' :catalog_id='catalog.id' />
											</td>
											<td class='num'>{{u.cnt}}</td>
										</tr>
									</tbody>
								</table>
							</div>
						</div>
					</div>

				</div>
				<div v-if='ext_url_pattern!=""' style='border-top:1px dotted black'>
					<small>
						External URL pattern: <tt>{{ext_url_pattern}}</tt>
					</small>
				</div>
			</div>
			<div v-else><i tt="loading"></i></div>
		</div>
		<div v-else-if='!loaded'><i tt="loading"></i></div>
	</div> <!-- wrapper -->
`
});
