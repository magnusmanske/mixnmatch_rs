import { mnm_api, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['sparql'],
	data: function () { return { entries: {}, entries_subset: {}, catalog: {}, sparql_query: '', loaded: false, per_page: 50, start: 0, number_of_entries: 0, loading: false } },
	created: function () { this.loadSparqlList(this.sparql) },
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		loadSparqlList: async function (sparql) {
			const me = this;
			me.loaded = false;
			if (typeof sparql != 'undefined') me.sparql_query = me.sparql;
			if (me.sparql_query == '') return;

			me.loading = true;
			try {
				let d = await mnm_api('sparql_list', { sparql: me.sparql_query });
				me.start = 0;
				me.number_of_entries = 0;
				Object.entries(d.data.entries).forEach(function ([k, v]) {
					if (typeof d.data.users[v.user] == 'undefined') return;
					d.data.entries[k].username = d.data.users[v.user].name;
					me.number_of_entries++;
				});
				me.entries = d.data.entries;
				me.showSubset(0);
			} catch (e) {
				console.log('Fail', e);
			} finally {
				me.loaded = true;
				me.loading = false;
				tt_update_interface();
			}

		},
		showSubset: function (page) {
			const me = this;
			var subset = {};
			var cnt = 0;
			me.start = page;
			var first = me.per_page * me.start;
			var entries_arr = Object.entries(me.entries);
			for (var i = 0; i < entries_arr.length; i++) {
				var k = entries_arr[i][0], v = entries_arr[i][1];
				cnt++;
				if ((cnt - 1) < first) continue;
				if ((cnt - 1) >= first + me.per_page) break;
				subset[k] = v;
			}
			me.entries_subset = subset;
		},
		setPage: function (offset) {
			this.showSubset(Math.floor(offset / this.per_page));
		},
		onRun: function () {
			const me = this;
			router.push('/sparql/' + encodeURIComponent(me.sparql_query));
		}
	},
	watch: {
		'$route'(to, from) {
			this.loadSparqlList(to.params.sparql);
		}
	},
	template: `
	<div>
		<mnm-breadcrumb :crumbs="[{text: 'SPARQL'}]"></mnm-breadcrumb>
		<div style='margin-bottom:20px;'>
			<p tt='sparql_list_intro'></p>
			<textarea v-model='sparql_query' style='width:100%' rows=5 tt_placeholder='sparql_placeholder'></textarea>
			<button class='btn btn-outline-primary' @click.prevent='onRun' tt='run'></button>
			<router-link class='btn btn-outline-secondary'
				to="/sparql/SELECT DISTINCT %3Fitem %3FitemLabel WHERE { %3Fpainting wdt%3AP195 wd%3AQ82941 %3B wdt%3AP31 wd%3AQ3305213 %3B wdt%3AP170 %3Fitem SERVICE wikibase%3Alabel { bd%3AserviceParam wikibase%3Alanguage 'en' }}"
				tt='example'></router-link>
		</div>
		<div v-if='loaded && number_of_entries>0'>
			<pagination v-if="number_of_entries > per_page" :offset="start*per_page" :items-per-page="per_page" :total="number_of_entries"
				:show-first-last="true" @go-to-page="setPage"></pagination>
			<div><entry-list-item v-for="e in entries_subset" :show_catalog=1 :entry="e" :hide_remove_on_automatch="1"
					:show_permalink="1" :key="e.id"></entry-list-item></div>
			<pagination v-if="number_of_entries > per_page" :offset="start*per_page" :items-per-page="per_page" :total="number_of_entries"
				@go-to-page="setPage"></pagination>
		</div>
		<div v-else-if='loaded'>
			<i tt='no_results'></i>
		</div>
		<div v-if='loading'>
			<i tt="loading"></i>
		</div>
	</div>
`
});
