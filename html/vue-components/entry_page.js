import { mnm_api, mnm_notify, mnm_loading, ensure_catalog, get_specific_catalog, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['id'],
	data: function () { return { entry: {}, catalog: {}, loaded: false, error: '' } },
	created: function () {
		this.loadEntry(this.id);
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		loadEntry: async function (id) {
			const me = this;
			me.loaded = false;
			me.error = '';
			mnm_loading(true);
			try {
				let d = await mnm_api('get_entry', { entry: id });
				if (typeof d.data.entries[id] == 'undefined') {
					me.error = 'Entry #' + id + ' not found';
					return;
				}
				Object.entries(d.data.entries).forEach(function ([k, v]) {
					if (typeof d.data.users[v.user] == 'undefined') return;
					d.data.entries[k].username = d.data.users[v.user].name;
				});
				me.entry = d.data.entries[id];
				await ensure_catalog(me.entry.catalog);
				me.catalog = get_specific_catalog(me.entry.catalog);
				me.loaded = true;
			} catch (e) {
				me.error = 'Failed to load entry #' + id + ': ' + (e.message || e);
			} finally {
				mnm_loading(false);
			}
		}
	},
	watch: {
		'$route'(to, from) {
			this.loadEntry(to.params.id);
		}
	},
	template: `
	<div>
		<div v-if='loaded'>
			<mnm-breadcrumb :crumbs="[
				{text: catalog.name, to: '/catalog/'+catalog.id},
				{text: 'Entry #'+entry.id}
			]"></mnm-breadcrumb>
			<catalog-header :catalog="catalog"></catalog-header>
			<entry-details :entry='entry'></entry-details>
			<match-entry :entry='entry'></match-entry>
		</div>
		<div v-else-if="error" class="alert alert-danger mt-3">{{error}}</div>
		<div v-else>
			<i tt="loading"></i>
		</div>
	</div>
`
});
