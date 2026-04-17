import { editEntryMixin } from './mnm-mixins.js';
import { mnm_api, mnm_fetch_json, mnm_notify, tt_update_interface, widar, tt } from './store.js';

// Inject leaflet CSS
(function () {
	if (!document.querySelector('link[href*="leaflet"]')) {
		const link = document.createElement('link');
		link.rel = 'stylesheet';
		link.href = 'https://tools-static.wmflabs.org/cdnjs/ajax/libs/leaflet/1.9.4/leaflet.min.css';
		document.head.appendChild(link);
	}
	const s = document.createElement('style');
	s.textContent = `span.wikidatamap_note { border-right: 1px dotted #ccc; }
span.wikidatamap_note:last-of-type { border-right: none; }`;
	document.head.appendChild(s);
})();

// ********************************************************************************

export function MapSource() {
	this.cache = {};
	this.name = '';
	this.status = '';
}

MapSource.prototype.load_all = function (callback) {
	if (typeof callback == "function") callback();
};

MapSource.prototype.iterate_all = function* () {
	let values = Object.values(this.cache);
	for (let element of values) {
		yield element;
	}
};

MapSource.prototype.getEntry = function (id) {
	return this.cache[id];
};

MapSource.prototype.mapHasChanged = function (data, callback) {
	callback(false);
};

// ********************************************************************************

MapSourceMnM.prototype = new MapSource;
MapSourceMnM.prototype.constructor = MapSourceMnM;
export function MapSourceMnM(catalog_id) {
	let self = this;
	MapSource.call(self);
	self.name = 'mnm';
	self.api = 'https://mix-n-match.toolforge.org/api.php';
	self.catalog_id = catalog_id;
	self.edit_mixin = JSON.parse(JSON.stringify(editEntryMixin));
	self.edit_mixin.methods = Object.assign({}, editEntryMixin.methods);
	mnm_fetch_json(self.api + '?' + new URLSearchParams({ query: 'get_catalog_info', catalog: self.catalog_id })).then(function (d) {
		self.catalog = d.data[0];
	});
}
MapSourceMnM.prototype.action_remove_match = async function (data, callback) {
	let self = this;
	let entry = data.entry.aux.entry;
	try {
		await mnm_api('remove_q', {
			tusc_user: widar.getUserName(),
			entry: entry.id
		}, { method: 'POST' });
		entry.q = null;
		entry.user = null;
		entry.username = null;
		entry.timestamp = null;
		data.entry.aux.status = 'unmatched';
		data.entry.desc = entry.ext_desc;
		data.entry.actions = [];
		data.entry.actions.push({ label: "Match to item", action: 'match_to_item', type: 'outline-primary' });
		data.entry.actions.push({ label: "Create new item", action: 'create_new_item', type: 'outline-success' });
		callback(true);
	} catch (e) {
		mnm_notify(e.message, 'danger');
		callback(false);
	}
};
MapSourceMnM.prototype.action_match_to_item = function (data, callback) {
	let self = this;
	let q = prompt(tt.t("enter_q_number"));
	if (q == null) return callback(false);
	q = "Q" + q.replace(/\D/, '');
	if (q == "Q") return callback(false);
	let entry = data.entry.aux.entry;
	let value = entry.ext_id;
	let prop;
	if (self.catalog.wd_prop != null && self.catalog.wd_qual == null) prop = 'P' + self.catalog.wd_prop;
	else {
		prop = "P973";
		value = entry.ext_url;
		if (value == null || value == '') {
			mnm_notify('Catalog has no property, and entry has no URL', 'danger');
			return callback(false);
		}
	}
	let summary = 'Matched to [[:toollabs:mix-n-match/#/entry/' + entry.id + '|' + entry.ext_name + ' (#' + entry.id + ')]]';
	let params = { botmode: 1, action: 'set_string', id: q, prop: prop, text: value, summary: summary };
	widar.run(params, function (d) {
		if (d.error != 'OK') {
			mnm_notify(d.error, 'danger');
			return callback(false);
		}
		data.entry.aux.status = 'fullmatch';
		data.entry.desc = entry.ext_desc + "\nMatched to [[" + q + "]]";
		data.entry.actions = [];
		data.entry.actions.push({ label: "Remove match", action: 'remove_match', type: 'outline-danger' });
		self.edit_mixin.methods.setEntryQ(entry, q, true, function (q) { callback(true); }, function () { callback(false); });
	});
};
MapSourceMnM.prototype.action_create_new_item = async function (data, callback) {
	let self = this;
	let entry = self.cache[data.entry.id];
	let entry_id = entry.aux.entry_id;
	try {
		let d = await mnm_fetch_json(self.api + '?' + new URLSearchParams({ query: 'prep_new_item', entry_ids: '' + entry_id }));
		if ((d.status || '') != 'OK' || typeof d.data == 'undefined') {
			mnm_notify('Problem creating item: ' + (d.status || 'ERROR'), 'danger');
			return callback();
		}
		let params = {
			action: 'wbeditentity',
			'new': 'item',
			data: d.data
		};
		let summary = 'New item based on [[:toollabs:mix-n-match/#/entry/' + entry_id + '|' + entry.ext_name + ' (#' + entry_id + ')]]';
		params = {
			action: 'generic',
			summary: summary,
			json: JSON.stringify(params)
		};
		widar.run(params, function (d) {
			if (d.error != 'OK') {
				mnm_notify(d.error, 'danger');
				return callback();
			}
			let q = d.res.entity.id.replace(/\D/g, '');
			if (typeof q == 'undefined' || q == 0 || q === null) {
				mnm_notify('Missing/invalid QID', 'danger');
				return callback();
			}
			entry.aux.status = 'fullmatch';
			entry.desc = entry.aux.entry.ext_desc + "\nMatched to [Q" + q + "]]";
			entry.actions = [];
			entry.actions.push({ label: "Remove match", action: 'remove_match', type: 'outline-danger' });
			self.edit_mixin.last_created_q = q;
			self.edit_mixin.methods.setEntryQ(entry.aux.entry, q, true, function (q) { callback(true); }, function () { callback(false); });
		});
	} catch (e) {
		mnm_notify('Problem creating item: ' + e.message, 'danger');
		callback();
	}
};
MapSourceMnM.prototype.mnm_entry2map_entry = function (entry) {
	let e = {
		id: '' + entry.id,
		label: entry.ext_name,
		desc: entry.ext_desc,
		url: entry.ext_url,
		url2: "https://mix-n-match.toolforge.org/#/entry/" + entry.id,
		lat: entry.lat,
		lon: entry.lon,
		aux: {
			entry_id: entry.id,
			entry: entry
		},
		actions: []
	};
	if (entry.user == null) {
		e.aux.status = 'unmatched';
		e.actions.push({ label: "Match to item", action: 'match_to_item', type: 'outline-primary' });
		e.actions.push({ label: "Create new item", action: 'create_new_item', type: 'outline-success' });
	} else if (entry.user == 0) {
		e.aux.status = 'automatch';
		e.aux.q = entry.q;
		e.desc += "\nPreliminarily matched to [[Q" + entry.q + "]]";
		e.actions.push({ label: "Confirm match", action: 'confirm_match', type: 'outline-primary' });
		e.actions.push({ label: "Remove match", action: 'remove_match', type: 'outline-danger' });
		e.actions.push({ label: "Create new item", action: 'create_new_item', type: 'outline-success' });
	} else {
		e.aux.status = 'fullmatch';
		e.desc += "\nMatched to [[Q" + entry.q + "]]";
		e.actions.push({ label: "Remove match", action: 'remove_match', type: 'outline-danger' });
	}
	return e;
};
MapSourceMnM.prototype.load_all = function (callback) {
	let self = this;
	if (self.fixed_entries) {
		self.fixed_entries.forEach(function (entry) {
			let e = self.mnm_entry2map_entry(entry);
			self.cache[e.id] = e;
		});
		if (typeof callback == "function") callback();
		return;
	}
	mnm_fetch_json(self.api + '?' + new URLSearchParams({ query: 'get_locations_in_catalog', catalog: self.catalog_id })).then(function (d) {
		Vue.set(self, 'status', 'loading');
		(d.data || []).forEach(function (entry) {
			let e = self.mnm_entry2map_entry(entry);
			self.cache[e.id] = e;
		});
		Vue.set(self, 'status', '');
		if (typeof callback == "function") callback();
	});
};

// ********************************************************************************

MapSourceWikidata.prototype = new MapSource;
MapSourceWikidata.prototype.constructor = MapSourceWikidata;
export function MapSourceWikidata(catalog_id) {
	MapSource.call(this);
	this.name = 'wikidata';
	this.sparql_url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql';
	this.sparql_limit = 5000;
	this.thumb_size = 200;
	this.min_zoom = 10;
	this.language = 'en';
	this.fixed_item_ids = null;
}
MapSourceWikidata.prototype.load_all = function (callback) {
	let self = this;
	if (!self.fixed_item_ids || self.fixed_item_ids.length == 0) {
		if (typeof callback == "function") callback();
		return;
	}
	let sparql = "SELECT ?q ?qLabel ?location ?image WHERE { VALUES ?q { " + self.fixed_item_ids.map(function (q) { return "wd:" + q; }).join(' ') + " } OPTIONAL { ?q wdt:P625 ?location } OPTIONAL { ?q wdt:P18 ?image } SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\" } }";
	mnm_fetch_json(self.sparql_url, { format: 'json', query: sparql }).then(function (d) {
		(d.results.bindings || []).forEach(function (item) {
			if (item.q.type != 'uri') return;
			let q = item.q.value.replace(/^.+\//, '');
			let e = { id: q, label: q, desc: '', url: 'https://www.wikidata.org/wiki/' + q, has_image: false, aux: {} };
			if (typeof item.location != 'undefined' && item.location.type == 'literal') {
				var m = item.location.value.match(/^Point\((.+?)\s(.+?)\)$/);
				if (m) { e.lat = m[2] * 1; e.lon = m[1] * 1; }
			}
			if (typeof item.qLabel != 'undefined') e.label = item.qLabel.value;
			self.cache[e.id] = e;
		});
		if (typeof callback == "function") callback();
	});
};
MapSourceWikidata.prototype.mapHasChanged = function (data, callback) {
	let self = this;
	if (self.fixed_item_ids) return callback(false);
	self.cache = {};
	if (data.zoom < self.min_zoom) {
		Vue.set(self, 'status', 'zoom in to view');
		return callback(true);
	}
	let bounds = data.bounds;
	var sparql = "#TOOL: Mix'n'match\n";
	sparql += 'SELECT ?q ?qLabel ?location ?image ?reason ?desc ?commonscat ?street WHERE { ';
	sparql += ' SERVICE wikibase:box { ?q wdt:P625 ?location . ';
	sparql += 'bd:serviceParam wikibase:cornerSouthWest "Point(' + bounds._southWest.lng + ' ' + bounds._southWest.lat + ')"^^geo:wktLiteral . ';
	sparql += 'bd:serviceParam wikibase:cornerNorthEast "Point(' + bounds._northEast.lng + ' ' + bounds._northEast.lat + ')"^^geo:wktLiteral }';
	sparql += ' OPTIONAL { ?q wdt:P18 ?image } ';
	sparql += ' OPTIONAL { ?q wdt:P373 ?commonscat } ';
	sparql += ' OPTIONAL { ?q wdt:P969 ?street } ';
	sparql += ' SERVICE wikibase:label { bd:serviceParam wikibase:language "' + self.language + ',en,de,fr,es,it,nl,el" . ?q schema:description ?desc . ?q rdfs:label ?qLabel } ';
	sparql += ' } LIMIT ' + self.sparql_limit;
	Vue.set(self, 'status', 'loading');
	mnm_fetch_json(self.sparql_url, { format: 'json', query: sparql }).then(function (d) {
		if (typeof d == 'undefined' || typeof d.results == 'undefined' || typeof d.results.bindings == 'undefined') return callback(false);
		let entries_returned = 0;
		(d.results.bindings || []).forEach(function (item) {
			entries_returned += 1;
			if (item.q.type != 'uri') return;
			let q = item.q.value.replace(/^.+\//, '');
			let e = { id: q, label: q, desc: '', url: 'https://www.wikidata.org/wiki/' + q, has_image: false, aux: {} };
			if (typeof item.location != 'undefined' && item.location.type == 'literal' && item.location.datatype == "http://www.opengis.net/ont/geosparql#wktLiteral") {
				var m = item.location.value.match(/^Point\((.+?)\s(.+?)\)$/);
				if (m == null) return;
				e.lat = m[2] * 1;
				e.lon = m[1] * 1;
			} else return;
			if (typeof item.qLabel != 'undefined' && item.qLabel.type == 'literal') e.label = item.qLabel.value;
			if (typeof item.desc != 'undefined' && item.desc.type == 'literal') e.desc = item.desc.value;
			if (typeof item.image != 'undefined') {
				if (item.image.type == 'uri') {
					let image_name = decodeURIComponent(item.image.value.replace(/^.+\//, ''));
					e.image = {
						thumbnail_url: 'https://commons.wikimedia.org/wiki/Special:Redirect/file/' + encodeURIComponent(image_name) + '?width=' + self.thumb_size + 'px&height=' + self.thumb_size + 'px',
						page_url: 'https://commons.wikimedia.org/wiki/File:' + encodeURIComponent(image_name)
					};
					e.has_image = true;
				}
			}
			self.cache[e.id] = e;
		});
		if (entries_returned >= self.sparql_limit) Vue.set(self, 'status', 'incomplete');
		else Vue.set(self, 'status', '');
		callback(true);
	});
};

// ********************************************************************************

// wikidata-map component (default export)
export default {
	name: 'wikidata-map',
	props: ['mapdata', 'entry'],
	data: function () { return { loaded: false, data: {}, map: {}, entry_focus: {} }; },
	created: function () {
		let self = this;
		self.loadLeaflet(function () {
			self.data = self.mapdata;
			self.createFromData();
			self.entry_focus = self.entry;
			self.focusOnEntry(self.entry_focus);
			self.loaded = true;
		});
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	watch: {
		"entry": function (val, oldVal) {
			let self = this;
			self.entry_focus = val;
			self.focusOnEntry(self.entry_focus);
		}
	},
	methods: {
		focusOnEntry: function (entry) {
			let self = this;
			if (typeof self.data.sources[entry.source] == 'undefined') return;
			let source_entry = self.data.sources[entry.source].getEntry(entry.id);
			if (typeof source_entry == 'undefined') return;
			let markerBounds = L.latLngBounds([L.latLng(source_entry.lat, source_entry.lon)]);
			self.map.fitBounds(markerBounds);
		},
		escapeHTML: function (str) {
			if (typeof str == 'undefined') return "";
			return new Option("" + str).innerHTML;
		},
		isLeafletLoaded: function (callback) {
			if (typeof L == 'undefined') {
				let self = this;
				setTimeout(function () { self.isLeafletLoaded(callback); }, 200);
				return;
			}
			callback();
		},
		loadLeaflet: function (callback) {
			if (typeof L == 'undefined') {
				const script = document.createElement('script');
				script.src = 'https://tools-static.wmflabs.org/cdnjs/ajax/libs/leaflet/1.9.4/leaflet.min.js';
				document.head.append(script);
			}
			this.isLeafletLoaded(callback);
		},
		updateLayers: function (ev) {
			let self = this;
			let bounds = self.map.getBounds();
			let zoom = self.map.getZoom();
			Object.entries(self.data.sources).forEach(function ([source, ds]) {
				ds.mapHasChanged({ bounds: bounds, zoom: zoom }, function (source_has_changed) {
					if (!source_has_changed) return;
					self.updateSourceLayers(source);
				});
			});
		},
		updateSourceLayers: function (source) {
			let self = this;
			let ds = self.data.sources[source];
			self.data.layers.forEach(function (layer) {
				if (layer.source != source) return;
				self.refillLayer(layer);
			});
		},
		refillLayer: function (layer) {
			let self = this;
			layer.entries = [];
			let source = self.data.sources[layer.source];
			let it = source.iterate_all();
			if (typeof layer.filter_entry != 'function') layer.filter_entry = function () { return true; };
			for (const entry of it) {
				if (layer.filter_entry(entry)) layer.entries.push(entry);
			}
			return self.addMarkersForLayer(layer);
		},
		addMarkersForLayer: function (layer) {
			let self = this;
			if (typeof layer.markers != 'undefined') {
				layer.map_layer.removeLayer(layer.markers);
				delete layer.markers;
			}
			if (typeof layer.entries == 'undefiend') return;
			let markers = new L.FeatureGroup();
			layer.markers = markers;
			layer.map_layer.addLayer(markers);
			layer.entries.forEach(function (entry, entry_num) {
				let title;
				let after_title = '';
				if (typeof entry.label == 'undefined' || entry.label == '') {
					title = "<i>" + self.escapeHTML(entry.id) + "</i>";
					if (typeof entry.url2 != 'undefined' && entry.url2 != '') after_title = " <small>[<a target='_blank' href='" + entry.url2 + "'>url</a>]</small>";
				} else {
					title = "<b>" + self.escapeHTML(entry.label) + "</b>";
					if (typeof entry.url2 != 'undefined' && entry.url2 != '') after_title = " <small>[<a target='_blank' href='" + entry.url2 + "'>" + self.escapeHTML(entry.id) + "</a>]</small>";
					else after_title = " <small>[" + self.escapeHTML(entry.id) + "]</small>";
				}
				if (typeof entry.url != 'undefined' && entry.url != '') title = "<a target='_blank' href='" + entry.url + "'>" + title + "</a>";
				title += after_title;

				let desc = entry.desc;
				desc = desc.replace(/\[\[(Q\d+)\]\]/, '<a target="_blank" href="https://www.wikidata.org/wiki/$1">$1</a>');

				let text = '';
				text += "<h6>" + layer.name + "</h6>";
				text += "<div>" + title + "</div>";
				text += "<div>" + self.escapeHTML(desc) + "</div>";
				text += "<div><tt>" + self.escapeHTML(entry.lat) + "/" + self.escapeHTML(entry.lon) + "</tt></div>";

				if (typeof entry.image != "undefined") {
					let image = "<img src='" + entry.image.thumbnail_url + "' />";
					text += "<div style='text-align:center'>" + image + "</div>";
					text = "<div style='width:200px'>" + text + "</div>";
				}

				if (typeof entry.actions != "undefined" && entry.actions.length > 0) {
					text += "<div style='display:flex;flex-wrap:wrap;'>";
					entry.actions.forEach(function (action) {
						let button = '<button type="button" class="btn btn-sm btn-' + (action.type || "outline-dark") + ' popup-button" action="' + action.action + '">' + self.escapeHTML(action.label) + '</button>';
						let container = "<div class='me-1 mb-1'>" + button + "</div>";
						text += container;
					});
					text += "</div>";
				}

				let options = { radius: (layer.radius || 10), weight: (layer.weight || 1), color: layer.color };
				let marker = L.circleMarker({ lat: entry.lat, lon: entry.lon }, options)
					.bindPopup(text)
					.on("popupopen", function (ev) { self.openMarker(ev, layer, entry, entry_num); })
					.addTo(markers);
				entry.marker = marker;
			});
			return markers.getBounds();
		},
		openMarker: function (ev, layer, entry, entry_num) {
			let self = this;
			let popup = ev.popup._container;
			popup.querySelectorAll('button.popup-button').forEach(function (button) {
				let action = button.getAttribute('action');
				button.addEventListener('click', function () { self.handle_action(action, layer, entry, entry_num); });
			});
		},
		handle_action: function (action, layer, entry, entry_num) {
			let self = this;
			let source = self.data.sources[layer.source];
			let fn_name = "action_" + action;
			if (typeof source[fn_name] == 'function') {
				source[fn_name]({ layer, entry, entry_num }, function (do_update) {
					if (do_update) self.updateSourceLayers(layer.source);
				});
			} else {
				console.log("Source " + layer.source + " has no function " + fn_name);
				mnm_notify('Not implemented yet', 'warning');
			}
		},
		createFromData: function () {
			let self = this;
			let map_element = self.$el.querySelector("div.wikidatamap_map");
			if (!map_element) {
				setTimeout(self.createFromData, 100);
				return;
			}
			let container_height = self.$el.offsetHeight;
			map_element.style.height = container_height + 'px';

			self.map = L.map(map_element, { drawControl: false });
			L.tileLayer('https://maps.wikimedia.org/osm-intl/{z}/{x}/{y}.png', { attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors' }).addTo(self.map);

			let bounds;
			self.map_layers = [];
			self.data.layers.forEach(function (layer) {
				let fg = L.featureGroup();
				let name = "<div style='display:inline-block;background-color:" + layer.color + ";border:1px solid " + layer.color + ";width:12px;height:12px;padding-top:3px;padding-right:3px;opacity:1.0;'></div> " + layer.name;
				self.map_layers[name] = fg;
				layer.map_layer = fg;
				let layer_bounds = self.refillLayer(layer);
				if (typeof bounds == 'undefined') bounds = layer_bounds;
				else bounds.extend(layer_bounds);
				if (layer.visible_from_start) fg.addTo(self.map);
			});
			self.layer_control = L.control.layers(null, self.map_layers).addTo(self.map);
			self.map.fitBounds(bounds);
			self.updateLayers();
			self.map.on('moveend', self.updateLayers);
			self.map.on('viewreset', self.updateLayers);
		}
	},
	template: `<div>
    <div v-if='loaded'>
        <div class='wikidatamap_map' style='width:100%; height:300px'></div>
        <div style="font-size:9pt;">
            <span v-for="(layer,layer_id) in data.layers" v-if='data.sources[layer.source].status!=""' class="wikidatamap_note">
                {{layer.name}}:{{data.sources[layer.source].status}}
            </span>
            &nbsp;
        </div>
    </div>
    <div v-else>
        <i tt='loading'></i>
    </div>
</div>`
};

