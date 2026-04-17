import { mnm_api, mnm_fetch_json, mnm_notify, get_all_catalogs, tt_update_interface, wd, tt, widar } from './store.js';

(function () {
	const s = document.createElement('style');
	s.textContent = `
.vm-page #app { max-width:none; margin:0; padding:0; }
.vm-header {
	position:fixed; top:1em; left:1em; right:1em; height:6em;
	border:1px solid #DDD; padding:3px; background:#fff; z-index:10;
}
.vm-entry {
	position:fixed; top:8em; left:1em; right:1em; height:5em;
	border:1px solid #DDD; padding:3px; background:#fff; z-index:10;
}
.vm-left {
	position:fixed; left:1em; top:14em; bottom:1em; width:35%;
	border:1px solid #DDD;
}
.vm-right {
	position:fixed; right:1em; top:14em; bottom:1em; width:35%;
	border:1px solid #DDD;
}
.vm-middle {
	position:fixed; left:37%; right:37%; top:14em; bottom:1em;
	border:1px solid #DDD; overflow:auto;
}
.vm-left iframe, .vm-right iframe {
	position:absolute; left:0; width:100%; height:100%; border:0;
}
.vm-search-result {
	margin:2px; margin-bottom:5px; padding-left:5px; padding-right:2px; cursor:pointer;
}
.vm-search-result:hover { background:#f0f0f0; }
.vm-search-result.vm-current { background-color:#CEDEF4; }
.vm-search-result.vm-has-prop { border-left:3px solid #FF4848; }
.vm-qdesc { font-size:9pt; color:#666; }
.vm-qimg { float:right; margin-left:3px; margin-bottom:2px; }
.vm-highlight { background-color:#FFFFAA; color:#666; padding:1px; }
`;
	document.head.appendChild(s);
})();

function escHtml(s) {
	var d = document.createElement('div');
	d.textContent = s;
	return d.innerHTML;
}

export default Vue.extend({
	props: ['id', 'catalog_param'],
	data: function () {
		return {
			catalog_id: null, catalogs: {}, entries: [], candidates: [],
			search_query: '', searching: false, auto_advance: false,
			selected_q: '', left_src: 'about:blank', right_src: 'about:blank',
			loading_entry: false, max_precache: 10
		};
	},
	created: function () {
		var me = this;
		// Parse route params/query
		if (me.id) {
			me.showEntry(me.id * 1);
			return;
		}
		if (me.catalog_param) {
			me.catalog_id = me.catalog_param * 1;
		}
		me.init();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () {
		document.body.classList.add('vm-page');
		tt_update_interface();
	},
	beforeDestroy: function () {
		document.body.classList.remove('vm-page');
	},
	computed: {
		catalog: function () {
			if (!this.catalog_id || !this.catalogs[this.catalog_id]) return null;
			return this.catalogs[this.catalog_id];
		},
		current_entry: function () {
			return this.entries.length > 0 ? this.entries[0] : null;
		},
		is_matched: function () {
			var e = this.current_entry;
			return e && e.q != null && e.user != 0;
		}
	},
	methods: {
		init: async function () {
			var me = this;
			try {
				var d = await mnm_api('catalogs');
				me.catalogs = d.data;
			} catch (e) {
				mnm_notify('Failed to load catalogs', 'danger');
				return;
			}
			if (me.catalog_id) {
				me.showRandomEntry();
			}
		},
		showEntry: async function (entry_id) {
			var me = this;
			me.loading_entry = true;
			me.entries = [];
			try {
				var d = await mnm_api('get_entry', { entry: entry_id });
				Object.values(d.data.entries).forEach(function (v) {
					me.entries.push(v);
					me.catalog_id = v.catalog;
				});
				// Make sure catalogs are loaded
				if (!me.catalogs[me.catalog_id]) {
					var d2 = await mnm_api('catalogs');
					me.catalogs = d2.data;
				}
				me.onEntryReady();
				me.showRandomEntry();
			} catch (e) {
				mnm_notify('Failed to load entry', 'danger');
			}
			me.loading_entry = false;
		},
		showRandomEntry: async function () {
			var me = this;
			if (!me.catalog_id) return;
			if (me.entries.length === 0) me.loading_entry = true;
			try {
				var d = await mnm_api('random', { catalog: me.catalog_id, submode: 'no_manual' });
				if (d.data.ext_url === '') {
					me.showRandomEntry();
					return;
				}
				me.entries.push(d.data);
				if (me.entries.length === 1) me.onEntryReady();
				if (me.entries.length < me.max_precache) me.showRandomEntry();
			} catch (e) { /* silent */ }
			me.loading_entry = false;
		},
		onEntryReady: function () {
			var me = this;
			if (!me.current_entry) return;
			var url = me.current_entry.ext_url.replace(/\bhttps?:/g, '');
			me.left_src = url;
			me.right_src = 'about:blank';
			me.selected_q = '';
			me.candidates = [];
			me.search_query = me.current_entry.ext_name;
			me.$nextTick(function () { me.runSearch(); });
		},
		loadNext: function () {
			var me = this;
			me.entries.shift();
			me.left_src = 'about:blank';
			me.right_src = 'about:blank';
			me.selected_q = '';
			me.candidates = [];
			if (me.entries.length > 0) me.onEntryReady();
			me.showRandomEntry();
		},
		runSearch: async function () {
			var me = this;
			if (!me.search_query) return;
			me.searching = true;
			me.candidates = [];
			me.selected_q = '';
			me.right_src = 'about:blank';
			try {
				var d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
					action: 'query', list: 'search', srnamespace: 0, srlimit: 500,
					srprop: 'snippet|redirecttitle', format: 'json', origin: '*',
					srsearch: me.search_query
				});
				var qs = [];
				var found_automatch = false;
				(d.query.search || []).forEach(function (v) {
					if (v.redirecttitle) return;
					me.candidates.push({ q: v.title, score: 0, label: v.title, desc: '', auto_desc: '', has_image: false, has_prop: false });
					qs.push(v.title);
					if (v.title.replace(/\D/g, '') == me.current_entry.q) found_automatch = true;
				});
				if (me.current_entry.q != null && !found_automatch) {
					var aq = 'Q' + me.current_entry.q;
					me.candidates.push({ q: aq, score: 0, label: aq, desc: '', auto_desc: '', has_image: false, has_prop: false });
					qs.push(aq);
				}
				if (me.candidates.length === 0) {
					me.searching = false;
					if (me.auto_advance) me.loadNext();
					return;
				}
				await wd.getItemBatch(qs);
				var lang = me.catalog ? me.catalog.search_wp : 'en';
				var rx = [];
				(me.current_entry.ext_desc || '').split(/[\s,\.+;:]+/).forEach(function (v) {
					if (v.length < 3) return;
					rx.push(new RegExp('\\b(' + v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')\\b', 'ig'));
				});
				me.candidates.forEach(function (c) {
					var i = wd.getItem(c.q);
					if (!i) return;
					var desc = i.getDesc(lang);
					if (!desc) desc = i.getDesc('en');
					rx.forEach(function (r) {
						var before = desc;
						desc = desc.replace(r, '<span class="vm-highlight">$1</span>');
						if (before !== desc) c.score += 10;
					});
					c.label = i.getLabel(lang) || c.q;
					c.desc = desc;
					c.has_image = i.getMultimediaFilesForProperty('P18').length > 0;
					if (c.has_image) c.score += 5;
					if (c.desc) c.score += 1;
					if (me.catalog && i.hasClaims(me.catalog.wd_prop)) {
						c.score -= 8;
						c.has_prop = true;
					}
				});
				me.candidates.sort(function (a, b) { return b.score - a.score; });
				// Auto-select first
				if (me.candidates.length > 0) me.selectCandidate(me.candidates[0].q);
				// Fetch auto-descriptions
				me.candidates.forEach(function (c) { me.fetchAutoDesc(c); });
			} catch (e) { /* silent */ }
			me.searching = false;
		},
		fetchAutoDesc: async function (candidate) {
			try {
				var d = await mnm_fetch_json('https://autodesc.toolforge.org/', {
					q: candidate.q, lang: 'en', mode: 'short', links: 'text', format: 'json'
				});
				var text = d.result || '';
				if (text === '<i>Cannot auto-describe</i>') text = '';
				candidate.auto_desc = this.highlightYears(text);
			} catch (e) { /* silent */ }
		},
		highlightYears: function (html) {
			if (!this.current_entry) return html;
			var desc = this.current_entry.ext_desc || '';
			var years = desc.match(/(\d{3,4})/g);
			if (!years) return html;
			years.forEach(function (year) {
				html = html.replace(new RegExp('\\b(' + year + ')\\b', 'ig'), '<span class="vm-highlight">$1</span>');
			});
			return html;
		},
		selectCandidate: function (q) {
			this.selected_q = q;
			this.right_src = '//m.wikidata.org/wiki/' + q;
		},
		doMatch: async function (q) {
			var me = this;
			q = ('' + q).replace(/\D/g, '');
			if (!q) return;
			var entry = me.current_entry;
			var do_create = (q === '0');

			// Edit Wikidata via WiDaR
			if (me.catalog && me.catalog.wd_prop != null) {
				var prop = 'P' + me.catalog.wd_prop;
				var j;
				if (do_create) {
					j = { claims: [{ mainsnak: { snaktype: 'value', property: prop, datavalue: { value: entry.ext_id, type: 'string' } }, type: 'statement', rank: 'normal' }] };
					var lang = me.catalog.search_wp;
					if (entry.ext_name) { j.labels = {}; j.labels[lang] = { language: lang, value: entry.ext_name }; }
					if (entry.ext_desc) { j.descriptions = {}; j.descriptions[lang] = { language: lang, value: entry.ext_desc }; }
					j = { action: 'wbeditentity', 'new': 'item', data: JSON.stringify(j) };
				} else {
					j = { action: 'wbcreateclaim', entity: 'Q' + q, property: prop, snaktype: 'value', value: JSON.stringify('' + entry.ext_id) };
				}
				try {
					var wd_result = await new Promise(function (resolve) {
						widar.run({ action: 'generic', json: JSON.stringify(j) }, function (d) { resolve(d); });
					});
					if (wd_result.error !== 'OK') {
						mnm_notify('Wikidata error: ' + wd_result.error, 'danger');
					}
					if (do_create && wd_result.res && wd_result.res.entity) {
						q = wd_result.res.entity.id.replace(/\D/g, '');
						window.open('https://www.wikidata.org/wiki/Q' + q, '_blank');
					}
				} catch (e) {
					mnm_notify('Wikidata error: ' + (e.message || e), 'danger');
				}
			}

			if (do_create && q === '0') return; // Creation failed

			// Set match in MnM
			try {
				await mnm_api('match_q', {
					tusc_user: widar.getUserName(), entry: entry.id, q: q
				}, { method: 'POST' });
			} catch (e) {
				mnm_notify("Mix'n'match error: " + (e.message || e), 'danger');
			}
			me.loadNext();
		},
		matchSelected: function () {
			if (this.selected_q) this.doMatch(this.selected_q);
		},
		createNew: function () {
			this.doMatch('0');
		},
		promptQ: function () {
			var reply = prompt(tt ? tt.t('enter_q_number') : 'Enter Q number', '');
			if (reply === null) return;
			var q = reply.replace(/\D/g, '');
			if (q) this.doMatch(q);
		}
	},
	watch: {
		'$route': function (to) {
			if (to.params.id) this.showEntry(to.params.id * 1);
			else if (to.params.catalog_param) {
				this.catalog_id = to.params.catalog_param * 1;
				this.entries = [];
				this.init();
			}
		}
	},
	template: `
<div>
	<!-- Header -->
	<div class='vm-header'>
		<div v-if='catalog' style='float:right'>
			<div v-if='widar && widar.is_logged_in'>{{widar.getUserName()}}</div>
			<div v-else><a href='/widar/index.php?action=authorize' target='_blank'>Log in</a></div>
			<label style='font-weight:normal;margin-top:5px'><input type='checkbox' v-model='auto_advance' /> Load next on empty results</label>
		</div>
		<h1 v-if='catalog' style='margin:0;padding:0'>
			<a :href='catalog.url' target='_blank'>{{catalog.name}}</a>
		</h1>
		<div v-if='catalog' style='font-size:12pt'>{{catalog.desc}}</div>
		<div v-else><i tt='loading'></i></div>
	</div>

	<!-- Entry bar -->
	<div class='vm-entry'>
		<div v-if='current_entry'>
			<div style='float:right'>
				<span v-if='is_matched' class='alert alert-danger' role='alert' style='display:inline-block;margin-right:10px'>
					Already matched to <a :href="'//www.wikidata.org/wiki/Q'+current_entry.q" target='_blank'>Q{{current_entry.q}}</a>
				</span>
				<div class='btn-group' role='group'>
					<button v-if='!is_matched' class='btn btn-warning btn-lg' title='Create a new Wikidata item for this entry' @click.prevent='createNew'>&#x2733;</button>
					<button v-if='!is_matched && selected_q' class='btn btn-success btn-lg' title='Match this entry to this item' @click.prevent='matchSelected'>&#x2713;</button>
					<button class='btn btn-secondary btn-lg' title='Load another one' @click.prevent='loadNext'>&#x21BB;</button>
				</div>
			</div>
			<h2 style='margin:0;padding:0;font-size:14pt'>
				<a :href='current_entry.ext_url' target='_blank'>{{current_entry.ext_name}}</a>
			</h2>
			<div>{{current_entry.ext_desc}}</div>
		</div>
		<div v-else-if='loading_entry'><i tt='loading'></i></div>
	</div>

	<!-- Left pane: external page -->
	<div class='vm-left'>
		<iframe :src='left_src'></iframe>
	</div>

	<!-- Middle pane: search -->
	<div class='vm-middle'>
		<div style='margin-bottom:1em'>
			<form @submit.prevent='runSearch'>
				<input style='width:100%' type='text' v-model='search_query' />
				<input style='width:100%' type='submit' class='btn btn-primary' value='Find' />
			</form>
		</div>
		<div v-if='searching'><i>Searching...</i></div>
		<div v-else-if='candidates.length===0 && current_entry'><i>No results found</i></div>
		<div v-for='c in candidates' :key='c.q'
			:class="'vm-search-result' + (c.q===selected_q ? ' vm-current' : '') + (c.has_prop ? ' vm-has-prop' : '')"
			@click='selectCandidate(c.q)'>
			<div v-if='c.has_image' class='vm-qimg'>&#x1F5BC;</div>
			<div>
				<a class='qlink'>{{c.label}}</a>
				[<small><a :href="'//www.wikidata.org/wiki/'+c.q" target='_blank'>{{c.q}}</a></small>]
			</div>
			<div class='vm-qdesc' v-html='c.desc'></div>
			<div class='vm-qdesc' v-if='c.auto_desc' v-html='c.auto_desc'></div>
			<div v-else class='vm-qdesc'>&mdash;</div>
		</div>
	</div>

	<!-- Right pane: Wikidata mobile view -->
	<div class='vm-right'>
		<iframe :src='right_src'></iframe>
	</div>
</div>
`
});
