import { mnm_api, tt_update_interface } from './store.js';

export default Vue.extend({
	data: function () {
		return {
			type_counts: [], latest_catalogs: [], top_groups: [],
			prop_groups_ig: [], prop_groups_country: [], prop_groups_loaded: false,
			maps_catalogs: [], maps_loaded: false,
			sort_field: 'count', sort_asc: false
		}
	},
	created: function () {
		this.init();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		init: function () {
			this.loadLatestCatalogs();
		},
		loadTypeCounts: async function () {
			if (this.type_counts.length > 0) return;
			var d = await mnm_api('catalog_type_counts');
			this.type_counts = d.data || [];
		},
		loadLatestCatalogs: async function () {
			var d = await mnm_api('latest_catalogs', { limit: 9 });
			this.latest_catalogs = d.data || [];
		},
		loadTopGroups: async function () {
			if (this.top_groups.length > 0) return;
			var d = await mnm_api('get_top_groups');
			this.top_groups = d.data || [];
		},
		loadPropertyGroups: async function () {
			if (this.prop_groups_loaded) return;
			var d = await mnm_api('catalog_property_groups');
			var ig = [], country = [];
			Object.entries(d.data || {}).forEach(function (entry) {
				var key = entry[0], g = entry[1];
				var item = { key: key, label: g.label, count: g.count };
				if (key.startsWith('ig_')) ig.push(item);
				else if (key.startsWith('country_')) country.push(item);
			});
			ig.sort(function (a, b) { return b.count - a.count; });
			country.sort(function (a, b) { return b.count - a.count; });
			this.prop_groups_ig = ig;
			this.prop_groups_country = country;
			this.prop_groups_loaded = true;
		},
		loadMaps: async function () {
			if (this.maps_loaded) return;
			var d = await mnm_api('catalogs_with_locations');
			this.maps_catalogs = d.data || [];
			this.maps_loaded = true;
		},
		goToCatalog: function (catalog) {
			this.$router.push('/catalog/' + catalog.id);
		},
		toggleSort: function (field) {
			if (this.sort_field === field) this.sort_asc = !this.sort_asc;
			else { this.sort_field = field; this.sort_asc = field === 'name'; }
		},
		sortedList: function (list) {
			let me = this;
			let sorted = list.slice();
			let field = me.sort_field;
			sorted.sort(function (a, b) {
				let va = field === 'name' ? (a.label || a.name || a.type || '').toLowerCase() : (a.count || a.cnt || 0) * 1;
				let vb = field === 'name' ? (b.label || b.name || b.type || '').toLowerCase() : (b.count || b.cnt || 0) * 1;
				if (va < vb) return me.sort_asc ? -1 : 1;
				if (va > vb) return me.sort_asc ? 1 : -1;
				return 0;
			});
			return sorted;
		},
		ucFirst: function (s) {
			return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, ' ');
		}
	},
	computed: {
		sortedTopGroups: function () {
			let me = this;
			let sorted = me.top_groups.slice();
			sorted.sort(function (a, b) {
				if (me.sort_field === 'name') {
					let va = a.name.toLowerCase(), vb = b.name.toLowerCase();
					if (va < vb) return me.sort_asc ? -1 : 1;
					if (va > vb) return me.sort_asc ? 1 : -1;
					return 0;
				} else {
					let va = a.catalogs.split(',').filter(function (c) { return c !== ''; }).length;
					let vb = b.catalogs.split(',').filter(function (c) { return c !== ''; }).length;
					return me.sort_asc ? va - vb : vb - va;
				}
			});
			return sorted;
		}
	},
	watch: {
		'$route'(to, from) { this.init(); },
	},
	template: `<div>
		<!-- Search catalogs -->
		<div class='mt-3 mb-3'>
			<h4 class='mb-0' tt='search_catalogs'></h4>
			<catalog-search-picker @select='goToCatalog'></catalog-search-picker>
		</div>

		<!-- Unified tabbed interface -->
		<ul class="nav nav-tabs flex-nowrap" style="overflow-x:auto;overflow-y:hidden;-webkit-overflow-scrolling:touch" role="tablist">
			<li class="nav-item">
				<a class="nav-link active" data-bs-toggle="tab" href="#mp-latest" role="tab"
					tt='latest_catalogs'></a>
			</li>
			<li class="nav-item">
				<a class="nav-link" data-bs-toggle="tab" href="#mp-groups" role="tab"
					@click.once='loadTypeCounts' tt='catalog_groups'></a>
			</li>
			<li class="nav-item">
				<a class="nav-link" data-bs-toggle="tab" href="#mp-propclass" role="tab"
					@click.once='loadPropertyGroups' style="white-space:nowrap">By property</a>
			</li>
			<li class="nav-item">
				<a class="nav-link" data-bs-toggle="tab" href="#mp-country" role="tab"
					@click.once='loadPropertyGroups' style="white-space:nowrap">By country</a>
			</li>
			<li class="nav-item">
				<a class="nav-link" data-bs-toggle="tab" href="#mp-topgroups" role="tab"
					@click.once='loadTopGroups' tt='top_groups'></a>
			</li>
			<li class="nav-item">
				<a class="nav-link" data-bs-toggle="tab" href="#mp-maps" role="tab"
					@click.once='loadMaps'>Maps</a>
			</li>
			<li class="nav-item">
				<a class="nav-link" data-bs-toggle="tab" href="#mp-about" role="tab"
					tt='about'></a>
			</li>
		</ul>

		<div class="tab-content mt-2">
			<!-- Latest catalogs (default, loaded on init) -->
			<div class="tab-pane fade show active" id="mp-latest" role="tabpanel">
				<div v-if='latest_catalogs.length==0'><i tt='loading'></i></div>
				<div v-else>
					<catalog-preview v-for='c in latest_catalogs' :key='c.id' :catalog='c'
						:link_to="'/catalog/'+c.id"></catalog-preview>
				</div>
			</div>

			<!-- Catalog groups (lazy) -->
			<div class="tab-pane fade" id="mp-groups" role="tabpanel">
				<div v-if='type_counts.length==0'><i tt='loading'></i></div>
				<table v-else class='table table-striped table-sm'>
					<thead><tr>
						<th style='width:100%;cursor:pointer' @click='toggleSort("name")' tt='group'></th>
						<th style='cursor:pointer' @click='toggleSort("count")' tt='catalogs'></th>
					</tr></thead>
					<tbody>
						<tr v-for='t in sortedList(type_counts)' :key='t.type'>
							<td><router-link :to="'/group/'+t.type">{{ucFirst(t.type)}}</router-link></td>
							<td class='num'>{{t.cnt}}</td>
						</tr>
					</tbody>
				</table>
			</div>

			<!-- By property class (lazy) -->
			<div class="tab-pane fade" id="mp-propclass" role="tabpanel">
				<div v-if='!prop_groups_loaded'><i tt='loading'></i></div>
				<table v-else class='table table-striped table-sm'>
					<thead><tr>
						<th style='width:100%;cursor:pointer' @click='toggleSort("name")' tt='group'></th>
						<th style='cursor:pointer' @click='toggleSort("count")' tt='catalogs'></th>
					</tr></thead>
					<tbody>
						<tr v-for='g in sortedList(prop_groups_ig)' :key='g.key'>
							<td><router-link :to="'/group/'+g.key">{{ucFirst(g.label)}}</router-link></td>
							<td class='num'>{{g.count}}</td>
						</tr>
					</tbody>
				</table>
			</div>

			<!-- By country (lazy) -->
			<div class="tab-pane fade" id="mp-country" role="tabpanel">
				<div v-if='!prop_groups_loaded'><i tt='loading'></i></div>
				<table v-else class='table table-striped table-sm'>
					<thead><tr>
						<th style='width:100%;cursor:pointer' @click='toggleSort("name")' tt='group'></th>
						<th style='cursor:pointer' @click='toggleSort("count")' tt='catalogs'></th>
					</tr></thead>
					<tbody>
						<tr v-for='g in sortedList(prop_groups_country)' :key='g.key'>
							<td><router-link :to="'/group/'+g.key">{{ucFirst(g.label)}}</router-link></td>
							<td class='num'>{{g.count}}</td>
						</tr>
					</tbody>
				</table>
			</div>

			<!-- Top groups (loaded on init) -->
			<div class="tab-pane fade" id="mp-topgroups" role="tabpanel">
				<p class='text-muted small mb-2'>
					<span tt='top_groups_blurb'></span>
					<router-link to='/top_groups' tt='manage_groups'></router-link>
				</p>
				<div v-if='top_groups.length==0'><i tt='loading'></i></div>
				<table v-else class='table table-striped table-sm'>
					<thead><tr>
						<th style='width:100%;cursor:pointer' @click='toggleSort("name")' tt='group'></th>
						<th style='cursor:pointer' @click='toggleSort("count")' tt='catalogs'></th>
					</tr></thead>
					<tbody>
						<tr v-for='g in sortedTopGroups' :key='g.id'>
							<td><router-link :to="'/top_groups/'+g.id">{{g.name}}</router-link></td>
							<td class='num'>{{g.catalogs.split(',').filter(c=>c!=='').length}}</td>
						</tr>
					</tbody>
				</table>
			</div>

			<!-- Maps (lazy) -->
			<div class="tab-pane fade" id="mp-maps" role="tabpanel">
				<div v-if='!maps_loaded'><i tt='loading'></i></div>
				<div v-else>
					<catalog-preview v-for='c in maps_catalogs' :key='c.id' :catalog='c'
						:link_to="'/map/'+c.id"></catalog-preview>
				</div>
			</div>

			<!-- About -->
			<div class="tab-pane fade" id="mp-about" role="tabpanel">
				<div class='lead'>
					<span tt='top_message'></span>
				</div>
			</div>
		</div>

	</div>`
});
