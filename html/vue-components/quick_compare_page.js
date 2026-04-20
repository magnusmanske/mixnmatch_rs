import { MapSourceMnM, MapSourceWikidata } from './wikidatamap.js';
import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, tt } from './store.js';

(function () {
	const s = document.createElement('style');
	s.textContent = `.td-center { text-align: center; }
.td-small { font-size: 9pt; }
table.quick_compare_table td { width: 50%; }`;
	document.head.appendChild(s);
})();

export default Vue.extend({
	props: ['catalog_id'],
	data: function () {
		return {
			loading: false, catalog: {}, entries: {}, entry: {}, mapdata_wd: {}, mapdata_ext: {},
			entry_focus_wd: {}, entry_focus_ext: {}, no_more_entries: false,
			require_coordinates: false, require_image: false,
			catalog_list: [], last_entry_id: 0
		};
	},
	created: function () {
		let me = this;
		if (typeof me.catalog_id == 'undefined') {
			me.load_list();
		} else {
			if (this.$route.query.require_coordinates) this.require_coordinates = true;
			if (this.$route.query.require_image) this.require_image = true;
			me.load_entries();
		}
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		async load_list() {
			let me = this;
			me.loading = true;
			try {
				let d = await mnm_api('quick_compare_list');
				me.catalog_list = d.data;
			} catch (e) {
				mnm_notify(e.message || 'Failed to load catalog list', 'danger');
			}
			me.loading = false;
		},
		update_own_url() {
			let url = "/quick_compare/" + this.catalog_id + '?';
			if (this.require_image) url += "&require_image=1";
			if (this.require_coordinates) url += "&require_coordinates=1";
			this.$router.push(url);
		},
		load_entries: function (load_next = true) {
			let me = this;
			if (me.no_more_entries) {
				me.loading = false;
				return;
			}
			me.loading = true;
			me.entries = {};
			mnm_api('quick_compare', {
				catalog: me.catalog_id,
				require_coordinates: this.require_coordinates ? 1 : 0,
				require_image: this.require_image ? 1 : 0,
			}).then(function (d) {
				if (d.data.entries.length == 0) me.no_more_entries = true;
				me.entries = d.data.entries;
				if (load_next) me.next_entry();
			}).catch(function (e) {
				me.no_more_entries = true;
				me.loading = false;
				mnm_notify(e.message || 'Failed to load entries', 'danger');
			});
		},
		next_entry: async function () {
			let me = this;
			let entry_ids = Object.keys(me.entries);
			if (entry_ids.length == 0) return this.load_entries();

			let entry_id = entry_ids[0];
			if (entry_id == me.last_entry_id) {
				delete me.entries[entry_id];
				return me.next_entry();
			}
			me.last_entry_id = entry_id;
			me.entry = me.entries[entry_id];
			if (Array.isArray(me.entry.ext_img)) me.entry.ext_img = me.entry.ext_img[0];
			delete me.entries[entry_id];
			await ensure_catalog(me.entry.catalog);
			me.catalog = get_specific_catalog(me.entry.catalog);
			if (entry_ids.length < 8) me.load_entries(false);

			me.mapdata_wd = { layers: [], sources: {} };
			me.mapdata_ext = { layers: [], sources: {} };
			let mnm = new MapSourceMnM(me.catalog_id);
			mnm.fixed_entries = [me.entry];
			Vue.set(me.mapdata_ext.sources, mnm.name, mnm);

			let wikidata = new MapSourceWikidata();
			wikidata.fixed_item_ids = ['Q' + me.entry.q];
			Vue.set(me.mapdata_wd.sources, wikidata.name, wikidata);

			Promise.all([
				new Promise(function (resolve, reject) { mnm.load_all(resolve); }),
				new Promise(function (resolve, reject) { wikidata.load_all(resolve); }),
			]).then(() => {
				let layer_base = { entries: [], visible_from_start: true, max: 2000 };
				me.mapdata_wd.layers.push(Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t('wikidata'), source: wikidata.name, color: "#2DC800" }));
				me.mapdata_ext.layers.push(Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t("mixnmatch"), source: mnm.name, color: "#2DFA00" }));
				me.loading = false;
			});
		},
		clear_and_next_entry: function () {
			this.update_own_url();
			this.entries = {};
			this.$router.go();
		},
		render_distance: function () {
			if (this.entry.distance_m < 100) {
				return Math.round(this.entry.distance_m) + 'm';
			} else {
				return (Math.round(this.entry.distance_m / 100) / 10) + 'km';
			}
		}
	},
	template: `<div class='mt-2'>
<mnm-breadcrumb v-if='typeof catalog != "undefined" && catalog && catalog.id' :crumbs="[
	{text: catalog.name, to: '/catalog/'+catalog.id},
	{text: 'Quick compare'}
]"></mnm-breadcrumb>
<catalog-header v-if='typeof catalog != "undefined" && catalog && catalog.id' :catalog="catalog"></catalog-header>
<div>
	<label v-if='catalog.has_locations=="yes"'>
		<input type='checkbox' v-model='require_coordinates' @change='clear_and_next_entry()' /> <span tt='require_coordinates'></span>
	</label>
	<label v-if='typeof catalog.image_pattern!="undefined"'>
		<input type='checkbox' v-model='require_image' @change='clear_and_next_entry()' /> <span tt='require_image'></span>
	</label>
</div>
<div v-if='loading'>
	<i tt='loading'></i>
</div>
<div v-else-if="typeof catalog_id=='undefined'">
	<table class="table">
		<tr>
			<th tt='catalog_name'></th>
			<th tt='description'></th>
			<th tt='auto_matched'></th>
		</tr>
		<tr v-for="c in catalog_list" v-if="c.autoq*1>0">
			<td>
				<router-link :to="'/quick_compare/'+c.id">{{c.name}}</router-link>
			</td>
			<td>
				{{c.desc}}
			</td>
			<td style="text-align: right; font-family: Courier">
				{{c.autoq}}
			</td>
		</tr>
	</table>
</div>
<div v-else-if='no_more_entries && entries.length==0'>
	<i tt="no_more_entries_left"></i>
</div>
<div v-else>
	<div>
		<table class='table quick_compare_table'>
			<tr>
				<td>
					External: <a target='_blank' :href='entry.ext_url' class='external'>{{entry.ext_id}}</a>
					(<a :href='"/#/entry/"+entry.id'>entry</a>)
				</td>
				<td>Wikidata: <a target='_blank' :href='"https://www.wikidata.org/wiki/"+entry.item.q' class='wikidata'>{{entry.item.q}}</a>
			</tr>
			<tr>
				<td class='td-center'>
					<div v-if='typeof entry.ext_img!="undefined" && entry.ext_img!=""'>
						<a :href='entry.ext_img' target='_blank'>
							<img border=0 :key='"img_entry_"+entry.id' :src='entry.ext_img' width='300px' style='max-height:250px;object-fit: contain;' />
						</a>
					</div>
				</td>
				<td class='td-center'>
					<div v-if='typeof entry.item.image!="undefined" && entry.item.image!=""'>
						<commons-thumbnail :key='"img_wd_"+entry.id' :filename='entry.item.image' width='300' heigth='250'></commons-thumbnail>
					</div>
				</td>
			</tr>
			<tr v-if="typeof entry.distance_m!='undefined'">
				<td style="position:relative; height:220px; overflow:hidden;">
					<wikidata-map :key='"map_entry_"+entry.id' :mapdata='mapdata_ext' :entry="entry_focus_ext" style='width:100%; height:200px'></wikidata-map>
				</td>
				<td style="position:relative; height:220px; overflow:hidden;">
					<wikidata-map :key='"map_wd_"+entry.id' :mapdata='mapdata_wd' :entry="entry_focus_wd" style='width:100%; height:200px'></wikidata-map>
				</td>
			</tr>
			<tr v-if="typeof entry.distance_m!='undefined'">
				<td colspan="2" style="text-align: center;">
					<span v-if='entry.distance_m<200' style='color:green;'>
						{{render_distance()}}
					</span>
					<span v-else style='color:red;'>
						{{render_distance()}}
					</span>
					distance
				</td>
			</tr>
			<tr>
				<td colspan="2">
					<entry-list-item :entry="entry" :show_catalog="1" :show_permalink="1" :twoline="1" :key="entry.id" ></entry-list-item>
				</td>
			</tr>
			<tr>
				<td colspan="2" style="text-align: right;">
					<button class='btn btn-outline-primary' tt='next' @click.prevent='next_entry'></button>
				</td>
			</tr>
		</table>
	</div>
</div>
</div>`
});
