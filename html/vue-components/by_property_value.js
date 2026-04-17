import { mnm_api, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['property', 'value'],
	data: function () { return { entries: {}, users: {}, loading: true } },
	created: function () {
		let me = this;
		me.load_entries();
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		load_entries: async function () {
			let me = this;
			me.loading = true;
			me.entries = [];
			let d = await mnm_api('entries_via_property_value', {
				property: me.property,
				value: me.value
			});
			me.entries = d.data.entries;
			me.users = d.data.users;
			for (const [k, v] of Object.entries(me.entries)) {
				v.username = '';
				if (typeof me.users[v.user] != 'undefined') {
					v.username = me.users[v.user].name;
				}
			}
			me.loading = false;
		}
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{text: 'P'+property+'='+value}]"></mnm-breadcrumb>
	<p tt='by_property_value_blurb'></p>
	<div v-if='loading'>
		<i tt='loading'></i>
	</div>
	<div v-else>
		<i v-if='Object.keys(entries).length === 0' tt='no_results'></i>
		<entry-list-item v-else v-for="e in entries" :entry="e" :show_catalog="1" :show_permalink="1" :twoline="1" :key="e.id"></entry-list-item>
	</div>

</div>
`
});
