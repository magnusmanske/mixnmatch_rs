import { mnm_api, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['gender'],
	data: function () { return { loading: true, use_gender: '', has_description: true } },
	created: function () {
		let me = this;
		if (typeof me.gender == 'undefined') me.use_gender = '';
		else me.use_gender = me.gender;
		me.load_entries();
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		load_entries: async function () {
			let me = this;
			me.loading = true;
			me.entries = [];
			let d = await mnm_api('random_person_batch', {
				gender: me.use_gender,
				has_desc: me.has_description ? 1 : 0
			});
			//console.log(JSON.parse(JSON.stringify(d)))
			me.entries = d.data;
			me.loading = false;
		}
	},
	watch: {
		use_gender: function (new_gender) {
			this.use_gender = new_gender;
			this.load_entries();
		},
		has_description: function (new_has_description) {
			this.has_description = new_has_description;
			this.load_entries();
		}
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{text: 'People'}]"></mnm-breadcrumb>
	<p tt='people_page_blurb'></p>
	<div>
		<span tt='gender'></span>:&nbsp;
		<label>
			<input type='radio' v-model='use_gender' value='' />
			<span tt='gender_any'></span>
		</label>
		<label>
			<input type='radio' v-model='use_gender' value='female' />
			<span tt='gender_female'></span>
		</label>
		<label>
			<input type='radio' v-model='use_gender' value='male' />
			<span tt='gender_male'></span>
		</label>
		<label>
			<input type='radio' v-model='use_gender' value='unknown' />
			<span tt='gender_unknown'></span>
		</label>
		<label>
			<input type='radio' v-model='use_gender' value='ambiguous' />
			<span tt='gender_ambiguous'></span>
		</label>
		<label>
			<input type='checkbox' v-model='has_description' value='1' />
			<span tt='has_description'></span>
		</label>
		<button style='float:right' class='btn btn-outline-primary' @click.prevent='load_entries()' tt='reload'></a>
	</div>
	<div v-if='loading'>
		<i tt='loading'></i>
	</div>
	<div v-else>
		<ol :start='1'>
			<li v-for='e in entries' style='border-bottom:1px solid #AAA'>
				<div v-if='e.name_count>1'>
					<router-link target='_blank' :to='"/creation_candidates/by_ext_name/?ext_name="+e.ext_name'><b>{{e.ext_name}}</b></router-link>
					({{e.name_count}} <span tt='entries_with_that_name'></span>)
				</div>
				<div v-else>
					<i tt='only_entry_with_that_name'></i>
				</div>
				<entry-list-item show_catalog='1' :entry="e" :show_permalink="1" :key="e.id"></entry-list-item>
			</li>
		</ol>
	</div>

</div>
`
});
