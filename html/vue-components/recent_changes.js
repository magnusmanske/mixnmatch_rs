import { mnm_api, mnm_notify, ensure_catalogs, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['catalog'],
	data: function () { return { entries: [], since: '0', loading: true } },
	created: function () { this.loadData(); },
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		getSinceTimestamp: function () {
			if (!this.since || this.since == '0') return '';
			let d = new Date();
			d.setDate(d.getDate() - parseInt(this.since));
			let pad = n => String(n).padStart(2, '0');
			return d.getFullYear() + pad(d.getMonth() + 1) + pad(d.getDate()) + '000000';
		},
		loadData: async function () {
			const me = this;
			me.loading = true;
			try {
				let params = {};
				if (typeof me.catalog != 'undefined') params.catalog = me.catalog * 1;
				let ts = me.getSinceTimestamp();
				if (ts) params.ts = ts;
				let d = await mnm_api('rc', params);
				Object.entries(d.data.events).forEach(function ([k, v]) {
					if (typeof d.data.users[v.user] == 'undefined') return;
					d.data.events[k].username = d.data.users[v.user].name;
				});
				var catalog_ids = [...new Set(Object.values(d.data.events).map(function (e) { return e.catalog; }).filter(Boolean))];
				await ensure_catalogs(catalog_ids);
				me.entries = d.data.events;
			} catch (e) {
				mnm_notify(e.message || 'Failed to load recent changes', 'danger');
			}
			me.loading = false;
		}
	},
	watch: {
		'$route'(to, from) {
			this.loadData();
		}
	},
	template: `
	<div>
		<mnm-breadcrumb :crumbs="[{tt: 'recent_changes'}]"></mnm-breadcrumb>
		<h2 tt='recent_changes'></h2>
		<div style='display:flex;align-items:baseline;gap:1em;margin-bottom:0.5em'>
			<a :href='"https://mix-n-match.toolforge.org/api.php?query=rc_atom"+(typeof catalog=="undefined"?"":"&catalog="+catalog)'
				tt='recent_changes_atom_feed'></a>
			<select v-model='since' @change='loadData' class='form-control' style='width:auto'>
				<option value='0'>All time</option>
				<option value='1'>Last 24 hours</option>
				<option value='7'>Last 7 days</option>
				<option value='30'>Last 30 days</option>
			</select>
		</div>
		<div v-if="loading"><i tt='loading'></i></div>
		<div v-else-if="entries.length==0"><i tt='no_results'></i></div>
		<div v-else>
			<catalog-header v-if='typeof catalog!="undefined"'
				:catalog="catalog"></catalog-header><!--Why does this not work??-->
			<div style="display:table;width:100%"><entry-list-item v-for="e in entries" :entry="e" :rc="1"
					:show_catalog="0" :show_permalink="1" :key="e.id"></entry-list-item></div>
		</div>
	</div>
`
});
