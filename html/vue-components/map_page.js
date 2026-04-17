import { MapSourceMnM, MapSourceWikidata } from './wikidatamap.js';
import { MapSourceCommons, MapSourceFlickr } from '../resources/vue_es6/wikidatamap.js';
import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, tt } from './store.js';

(function () {
	const s = document.createElement('style');
	s.textContent = `form.test_entry_form > div { margin-right:0.2rem; }
.mnm-map-container { min-height: 70vh; }
.mnm-map-container .wikidatamap_map { height: 70vh !important; }
@media (max-width: 767.98px) {
    .mnm-map-container { min-height: 50vh; }
    .mnm-map-container .wikidatamap_map { height: 50vh !important; }
}`;
	document.head.appendChild(s);
})();

export default Vue.extend({
	props: ['id', 'entry_id'],
	data: function () {
		return {
			loaded: false, selected_entry: {}, selected_item: {}, mapdata: {
				layers: [],
				sources: {}
			}, entry_focus: {}, flickr_key: ''
		};
	},
	created: async function () {
		let self = this;
		await ensure_catalog(self.id);
		self.catalog = get_specific_catalog(self.id);
		let wikidata = new MapSourceWikidata();
		Vue.set(self.mapdata.sources, wikidata.name, wikidata);
		let commons = new MapSourceCommons();
		Vue.set(self.mapdata.sources, commons.name, commons);
		let mnm = new MapSourceMnM(self.id);
		Vue.set(self.mapdata.sources, mnm.name, mnm);
		Promise.all([
			new Promise(function (resolve, reject) { mnm.load_all(resolve); }),
			mnm_api('get_flickr_key').then(function (d) {
				self.flickr_key = (d.data || '').trim();
			}).catch(function (e) {
				mnm_notify(e.message || 'Request failed', 'danger');
			}),
		]).then(() => {
			let flickr = new MapSourceFlickr(self.flickr_key);
			Vue.set(self.mapdata.sources, flickr.name, flickr);

			let layer_base = { entries: [], visible_from_start: true, max: 2000 };
			let layer;

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t('commons'), source: commons.name, color: "#F4CBA1" });
			layer.filter_entry = function (entry) { return entry.has_image; };
			self.mapdata.layers.push(layer);

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t('wikidata_with_images'), source: wikidata.name, color: "#2DC800" });
			layer.filter_entry = function (entry) { return entry.has_image; };
			self.mapdata.layers.push(layer);

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t("wikidata_without_images"), source: wikidata.name, color: "#FF4848" });
			layer.filter_entry = function (entry) { return !entry.has_image; };
			self.mapdata.layers.push(layer);

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t("unmatched"), source: mnm.name, color: "#4F00FA" });
			layer.filter_entry = function (entry) { return entry.aux.status == 'unmatched'; };
			self.mapdata.layers.push(layer);

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t("auto_matched"), source: mnm.name, color: "#2DFA00" });
			layer.filter_entry = function (entry) { return entry.aux.status == 'automatch'; };
			self.mapdata.layers.push(layer);

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t("manually_matched"), source: mnm.name, color: "#FF48FF" });
			layer.filter_entry = function (entry) { return entry.aux.status == 'fullmatch'; };
			self.mapdata.layers.push(layer);

			layer = Object.assign(JSON.parse(JSON.stringify(layer_base)), { name: tt.t('flickr'), source: flickr.name, color: "#8EB4E6" });
			self.mapdata.layers.push(layer);

			self.loaded = true;
			if (typeof self.entry_id != 'undefined') self.showMnMEntry(self.entry_id);
		});
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		onOpenMarker: function (ev) {
			let self = this;
			if (ev.layer.source == 'mnm') {
				self.selected_entry = ev.entry.aux.entry;
			} else if (ev.layer.source == 'wikidata') {
				self.selected_item = ev.entry;
			}
		},
		onDoubleClickMarker: function (ev) {
			let self = this;
			if (ev.layer.source == 'mnm') {
				self.showMnMEntry(ev.entry_num);
			}
		},
		showRandomUnmatchedEntry: function () {
			let self = this;
			self.selected_entry = {};
			self.selected_item = {};
			let entry_ids = [];
			Object.entries(self.mapdata.sources.mnm.cache).forEach(function ([id, entry]) {
				if (entry.aux.status == 'unmatched' || entry.aux.status == 'automatch') entry_ids.push(entry.id);
			});
			if (entry_ids.length == 0) {
				mnm_notify(tt.t("no_more_entries"), 'warning');
				return;
			}
			let entry_id = entry_ids[Math.floor(Math.random() * entry_ids.length)];
			self.showMnMEntry(entry_id);
		},
		showMnMEntry: function (entry_id, source) {
			let self = this;
			Vue.set(self, 'entry_focus', { id: '' + entry_id, source: 'mnm' });
			let entry = self.mapdata.sources.mnm.cache[entry_id].aux.entry;
			self.selected_entry = entry;
			if (entry.q != null) self.selected_item = { id: 'Q' + entry.q, desc: '' };
		}
	},
	template: `<div class='mt-2'>
    <mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
        {text: catalog.name, to: '/catalog/'+catalog.id},
        {text: 'Map'}
    ]"></mnm-breadcrumb>
    <div v-if='loaded'>
        <catalog-header :catalog="catalog"></catalog-header>
        <div class='mnm-map-container'><wikidata-map :mapdata='mapdata' :entry="entry_focus" @open-marker="onOpenMarker($event)" @double-click-marker="onDoubleClickMarker($event)" style='width:100%'></wikidata-map></div>
        <div class='mb-2 row'>
            <button class='btn btn-outline-info' tt='random_unmatched_entry' @click.prevent='showRandomUnmatchedEntry'></button>
        </div>
        <div v-if="typeof selected_item.id!='undefined'" class='mb-2 row' :key="'selected_item_'+selected_item.id">
            <div class="col-12"><span>{{selected_item.id}}</span>: <wd-link :item="selected_item.id"></wd-link></div>
            <div class="col-12">{{selected_item.desc}}</div>
        </div>
        <div v-if="typeof selected_entry.id!='undefined'" class='mb-2 row' style="display:table;width:100%">
            <entry-list-item :entry="selected_entry" :key="selected_entry.id"></entry-list-item>
        </div>
    </div>
</div>`
});
