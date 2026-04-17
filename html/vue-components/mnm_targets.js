import { mnm_api, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['property'],
	data: function () { return { loading: true, offset: 0, use_property: 0, batch_size: 25 } },
	created: function () {
		let me = this;
		me.offset = this.$route.query.offset ?? 0;
		if (typeof me.property != 'undefined') me.use_property = me.property * 1;
		mnm_api('mnm_unmatched_relations', {
			property: me.use_property,
			offset: me.offset
		}).then(function (d) {
			// console.log(JSON.parse(JSON.stringify(d)))
			me.loading = false;
			me.data = d.data;
		});

	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		reload: function (new_batch) {
			let offset = new_batch * this.batch_size;
			let path = '/mnm_targets/' + this.use_property + '?offset=' + offset;
			console.log(path);
			this.$router.push(path);
			this.$router.go();
		}
	},
	template: `
<div class='mt-2' :key='"mnm_targets_"+use_property+":"+offset' >
<mnm-breadcrumb :crumbs="[{text: 'MnM targets'}]"></mnm-breadcrumb>
<div v-if='loading'>
	<i tt='loading'></i>
</div>
<div v-else>
	<batch-navigator :key='"nav1_"+use_property+":"+offset' :batch_size='batch_size' total='1000' :current='offset/batch_size' @set-current='reload'></batch-navigator>
	<div style="display: flex;">
		<div style="min-width: 3rem;" tt="entries">
		</div>
		<div></div>
	</div>
	<div v-for="entry_id in data.entry_order" style="display: flex;">
		<div style="min-width: 3rem;">
			{{data.entry2cnt[entry_id]}}
		</div>
		<entry-list-item :entry="data.entries[entry_id]" :show_catalog="1" :show_permalink="1" :twoline="1" key="entry_id"></entry-list-item>
	</div>
	<batch-navigator :key='"nav2_"+use_property+":"+offset' :batch_size='batch_size' total='1000' :current='offset/batch_size' @set-current='reload'></batch-navigator>
</div>

</div>
`
});
