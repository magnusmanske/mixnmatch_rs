export default {
	name: 'catalog-header',
	props: ['catalog', 'nolink'],
	template: `
	<div>
		<div v-if='catalog.id'>
			<div class='d-flex justify-content-between align-items-start flex-wrap gap-2 mb-2'>
				<h1 class='mb-0'>
					<span v-if='nolink'>{{catalog.name}}</span>
					<a v-else :href='"#/catalog/"+catalog.id'>{{catalog.name}}</a>
				</h1>
				<div class="btn-group flex-shrink-0">
					<button type="button" class="btn btn-outline-secondary dropdown-toggle" data-bs-toggle="dropdown"
						aria-haspopup="true" aria-expanded="false">Action</button>
					<catalog-actions-dropdown v-bind:catalog="catalog"></catalog-actions-dropdown>
				</div>
			</div>

			<p class='text-muted' v-if="catalog.url"><a :href="catalog.url" class="external" target="_blank">{{catalog.desc}}</a></p>
			<p class='text-muted' v-else>{{catalog.desc}}</p>
		</div>
	</div>
`
};
