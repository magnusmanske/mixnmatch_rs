import { mnm_api, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['property', 'value'],
	data: function () {
		return {
			entries: {},
			users: {},
			loading: false,
			form_property: '',
			form_value: '',
		};
	},
	created: function () {
		const me = this;
		me.form_property = me.property || '';
		me.form_value = me.value || '';
		if (me.property && me.value) me.load_entries();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		submit: function () {
			const p = (this.form_property || '').replace(/\D/g, '');
			const v = (this.form_value || '').trim();
			if (!p || !v) return;
			this.$router.push('/by_property_value/' + encodeURIComponent(p) + '/' + encodeURIComponent(v));
		},
		load_entries: async function () {
			const me = this;
			me.loading = true;
			me.entries = {};
			const d = await mnm_api('entries_via_property_value', {
				property: me.property,
				value: me.value
			});
			me.entries = d.data.entries;
			me.users = d.data.users;
			for (const [, v] of Object.entries(me.entries)) {
				v.username = (me.users[v.user] || {}).name || '';
			}
			me.loading = false;
		}
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="property && value ? [{text: 'P'+property+'='+value}] : [{text:'Search by auxiliary value'}]"></mnm-breadcrumb>

	<div class='card mb-3'>
		<div class='card-body'>
			<h5 class='card-title'>Search by auxiliary property value</h5>
			<p class='text-muted small'>Find Mix\'n\'Match entries that carry a specific property value in their auxiliary data.</p>
			<form class='row g-2 align-items-end' @submit.prevent='submit'>
				<div class='col-auto'>
					<label class='form-label'>Property</label>
					<div class='input-group' style='width:10rem'>
						<span class='input-group-text'>P</span>
						<input type='number' min='1' class='form-control' v-model='form_property' placeholder='214' />
					</div>
				</div>
				<div class='col'>
					<label class='form-label'>Value</label>
					<input type='text' class='form-control' v-model='form_value' placeholder='e.g. 46552284' />
				</div>
				<div class='col-auto'>
					<button type='submit' class='btn btn-primary'>Search</button>
				</div>
			</form>
		</div>
	</div>

	<template v-if='property && value && !loading'>
		<i v-if='Object.keys(entries).length === 0' tt='no_results'></i>
		<entry-list-item v-else v-for='e in entries' :entry='e' :show_catalog='1' :show_permalink='1' :twoline='1' :key='e.id'></entry-list-item>
	</template>
</div>
`
});
