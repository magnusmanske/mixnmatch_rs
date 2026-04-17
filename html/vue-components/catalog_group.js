import { mnm_api, mnm_loading, tt_update_interface, specific_catalogs_cache } from './store.js';

export default Vue.extend({
	props: ["key", "order"],
	data: function () { return { slices: [], types: [], all_data: {}, only_with_prop: false, completed: '', filtering: false } },
	created: function () {
		this.init();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		init: async function () {
			const me = this;
			me.slices = [];
			me.only_with_prop = !!me.$route.query.prop;
			// Load type list for the dropdown
			mnm_api('catalog_type_counts').then(function (d) {
				me.types = ['all'];
				(d.data || []).forEach(function (v) { me.types.push(v.type); });
				me.types.sort();
			});
			// Load catalogs for this group
			var group = me.key || 'all';
			mnm_loading(true);
			try {
				var d = await mnm_api('catalog_by_group', { group: group });
				me.all_data = d.data || {};
				var numFields = ['total', 'manual', 'autoq', 'nowd', 'noq', 'na'];
				Object.entries(me.all_data).forEach(function (entry) {
					var id = entry[0], cat = entry[1];
					numFields.forEach(function (f) { cat[f] = Number(cat[f]) || 0; });
					cat.unmatched = cat.total - cat.manual - cat.autoq - cat.nowd - cat.na;
					specific_catalogs_cache[id] = cat;
				});
				me.rebuildSlices();
			} finally {
				mnm_loading(false);
			}
		},
		isItemGroup: function () {
			return /^(country|ig)_/.test(this.key);
		},
		calculateSlices: function (data) {
			const me = this;
			var slices = [];

			var by_type = {};
			if (!me.key || me.key == 'all') by_type.all = [];
			Object.entries(data).forEach(function ([k, v]) {
				if (v.type != me.key && me.key != 'all' && me.key) return;
				if (typeof by_type[v.type] == 'undefined') by_type[v.type] = [];
				by_type[v.type].push(v.id);
				if (!me.key || me.key == 'all') by_type.all.push(v.id);
			});

			var order_list = [];
			Object.keys(by_type).forEach(function (k) { order_list.push(k); });
			order_list.sort(function (a, b) { return by_type[b].length - by_type[a].length; });

			if (me.isItemGroup()) {
				var ids = [];
				Object.values(data).forEach(function (v) { ids.push(v.id); });
				slices.push({ title: me.ucFirst((me.key || '').replace(/^(ig|country)_/, '').replace(/_/g, ' ')), catalogs: ids });
			} else {
				order_list.forEach(function (type) {
					slices.push({ title: me.ucFirst(type), catalogs: by_type[type] });
				});
			}

			var sort_order = me.order;
			if (typeof me.order == 'undefined' || me.order == '') sort_order = 'order_easy';
			slices.forEach(function (slice) {
				slice.catalogs = slice.catalogs.sort(function (a, b) {
					if (sort_order == 'order_id') return a * 1 - b * 1;
					else if (sort_order == 'order_easy') return me.getEasy(specific_catalogs_cache[a]) - me.getEasy(specific_catalogs_cache[b]);
					else if (sort_order == 'order_alpha') return (specific_catalogs_cache[a].name || '').toUpperCase().localeCompare((specific_catalogs_cache[b].name || '').toUpperCase());
				});
			});

			if (me.completed == 'completed_section') {
				var completed = [];
				slices.forEach(function (v, k) {
					var keep = [];
					v.catalogs.forEach(function (v2) {
						if (me.isComplete(specific_catalogs_cache[v2])) completed.push(v2);
						else keep.push(v2);
					});
					slices[k].catalogs = keep;
				});
				slices.push({ title: 'Completed', catalogs: completed });
			}

			slices.forEach(function (slice, k) {
				slices[k].order = sort_order;
				slices[k].key = slice.title + '/' + sort_order;
				slices[k].section = 'section' + k;
			});

			me.slices = slices;
		},
		isComplete: function (catalog) {
			return catalog && (catalog.noq + catalog.autoq) == 0;
		},
		getEasy: function (catalog) {
			if (!catalog) return 999999999;
			var ret = Math.abs(catalog.noq) * 4 + Math.abs(catalog.autoq) * 2;
			if (ret == 0) ret = catalog.total * 10000000;
			return ret;
		},
		rebuildSlices: function () {
			let me = this;
			let data = me.all_data;
			if (me.only_with_prop) {
				data = {};
				Object.entries(me.all_data).forEach(function ([k, v]) {
					if (v.wd_prop != null && v.wd_prop != '0' && v.wd_prop != '') data[k] = v;
				});
			}
			me.calculateSlices(data);
		},
		togglePropFilter: function () {
			let me = this;
			me.filtering = true;
			me.slices = [];
			var query = me.only_with_prop ? { prop: 1 } : {};
			me.$router.replace({ query: query }).catch(function () {});
			Vue.nextTick(function () {
				me.rebuildSlices();
				me.filtering = false;
			});
		},
		setGroupFromSelect: function () {
			const me = this;
			var new_group = document.getElementById('select_catalog_group').value;
			var path = '/group/' + new_group;
			if (typeof me.order != 'undefined') path += '/' + me.order;
			me.$router.push({ path: path });
		},
		ucFirst: function (s) {
			return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, ' ');
		}
	},
	watch: {
		'$route'(to, from) { this.init() }
	},
	template: `
	<div>
		<mnm-breadcrumb :crumbs="[{text: 'Catalog groups'}]"></mnm-breadcrumb>
		<div>
			<div style='float:right'>
				<form class='form'>
					<select id='select_catalog_group' class="form-select" v-on:change="setGroupFromSelect">
						<option v-for='group in types' :value='group' :selected='group==key'>{{ucFirst(group)}}</option>
					</select>
				</form>
			</div>
			<span v-if="order=='order_id'" tt='by_id'></span><router-link v-else :to="'/group/'+key+'/order_id'"
				tt='by_id'></router-link> |
			<span v-if="order=='order_easy'||typeof order=='undefined'" tt='by_easy'></span><router-link v-else
				:to="'/group/'+key+'/order_easy'" tt='by_easy'></router-link> |
			<span v-if="order=='order_alpha'" tt='by_alpha'></span><router-link v-else
				:to="'/group/'+key+'/order_alpha'" tt='by_alpha'></router-link>
			| <label style='cursor:pointer;font-weight:normal'>
				<input type='checkbox' v-model='only_with_prop' @change='togglePropFilter' />
				<small tt='only_with_wd_property'>Only with WD property</small>
			</label>
			<hr />
		</div>

		<div v-if='filtering' class='text-center py-3'><i tt='loading'></i></div>
		<div v-else-if='slices.length > 0'><catalog-slice v-for="slice in slices" :catalogs="slice.catalogs" :title="slice.title"
				:section='slice.section' :key="slice.key"></catalog-slice></div>
		<div v-else><i tt='loading'></i></div>

	</div>
`
});
