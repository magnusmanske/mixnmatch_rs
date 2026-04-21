export default {
	name: 'catalog-slice',
	props: ['catalogs', 'title', 'order', 'section'],
	methods: {
		getTopMissingURL: function () {
			const me = this;
			return '/top_missing/' + me.catalogs.join(',');
		}
	},
	template: `
	<div class='slice mb-4' :id='section'>
		<div class='d-flex justify-content-between align-items-center mb-2'>
			<h4 class='mb-0'>{{title}}</h4>
			<router-link :to="getTopMissingURL()" class='btn btn-outline-primary btn-sm' tt='top_missing'></router-link>
		</div>
		<div class='progress-legend mb-1'>
			<span class='pl-manual' tt='manually_matched'></span>
			<span class='pl-auto' tt='auto_matched'></span>
			<span class='pl-nowd' tt='not_on_wikidata'></span>
			<span class='pl-na' tt='not_applicable'></span>
		</div>
		<div class="cat-table"><catalog-list-item v-for="cid in catalogs" v-bind:cid="cid"
				:key='cid'></catalog-list-item></div>
	</div>
`
};
