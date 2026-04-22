export default {
	name: 'catalog-header',
	props: ['catalog', 'nolink'],
	template: `
	<div>
		<div v-if='catalog.id'>
			<div class='mb-2' style='overflow:hidden'>
				<!--
					Float the dropdown to the right so the heading text
					wraps around it independently. The previous
					flex + flex-wrap layout pushed the whole dropdown
					onto a new line as soon as the title stopped fitting
					in one row.
				-->
				<div class='btn-group float-end ms-2 mt-1'>
					<button type='button' class='btn btn-outline-secondary dropdown-toggle'
						data-bs-toggle='dropdown'
						aria-haspopup='true' aria-expanded='false'>Action</button>
					<catalog-actions-dropdown v-bind:catalog='catalog'></catalog-actions-dropdown>
				</div>
				<h1 class='mb-0' style='word-break:break-word'>
					<span v-if='nolink'>{{catalog.name}}</span>
					<a v-else :href='"#/catalog/"+catalog.id'>{{catalog.name}}</a>
				</h1>
			</div>

			<p class='text-muted' v-if="catalog.url"><a :href="catalog.url" class="external" target="_blank">{{catalog.desc}}</a></p>
			<p class='text-muted' v-else>{{catalog.desc}}</p>
		</div>
	</div>
`
};
