import { ensure_catalog, get_specific_catalog } from './store.js';

export default {
	name: 'catalog-list-item',
	props: ['cid'],
	data: function () { return { catalog: {} } },
	created: async function () {
		await ensure_catalog(this.cid);
		this.catalog = get_specific_catalog(this.cid);
	},
	methods: {
		click: function (id, mode, mode2) {
			if (typeof mode2 == 'undefined') router.push('/' + mode + '/' + id);
			else router.push('/' + mode + '/' + id + '/' + mode2);
			return false;
		},
		renderPercentage: function (v) {
			const me = this;
			if (!me.catalog.total) return '';
			var ret = Math.floor(100 * v / me.catalog.total);
			if (ret == 0) return '';
			return ret + "%";
		}
	},
	template: `
	<div v-if="catalog.active==1" class="cat-row" v-once>

		<div class='catalog-list-item'
			:style="catalog.wd_prop*1>0?'border-left:1px solid white':'border-left:1px solid red'">
			<div class="btn-group">
				<router-link class="btn btn-light btn-sm" :to='"/catalog/"+catalog.id'
					style="text-align:left;width:200px;overflow:hidden"
					:title="catalog.name">{{catalog.name}}</router-link>
				<button type="button" class="btn btn-light btn-sm dropdown-toggle dropdown-toggle-split"
					data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false"></button>
				<catalog-actions-dropdown :catalog="catalog"></catalog-actions-dropdown>
			</div>
		</div>

		<div class="cat-cell-desc" :title="catalog.desc">
			<div style="overflow:hidden;white-space:nowrap;">
				<div v-if="catalog.url"><a :href="catalog.url" class="external" target="_blank">{{catalog.desc}}</a>
				</div>
				<div v-else>{{catalog.desc}}</div>
			</div>
		</div>

		<div class="cat-cell-progress">
			<div class="progress">
				<div role="progressbar" class="progress-bar bg-success"
					:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(100*catalog.manual/catalog.total):0)+'%'}">
					{{renderPercentage(catalog.manual)}}</div>
				<div role="progressbar" class="progress-bar bg-info"
					:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(100*catalog.autoq/catalog.total):0)+'%'}">
					{{renderPercentage(catalog.autoq)}}</div>
				<div role="progressbar" class="progress-bar bg-warning"
					:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(100*catalog.nowd/catalog.total):0)+'%'}">
					{{renderPercentage(catalog.nowd)}}</div>
				<div role="progressbar" class="progress-bar bg-danger"
					:style="{'white-space':'nowrap',width:(catalog.total?Math.floor(100*catalog.na/catalog.total):0)+'%'}">
					{{renderPercentage(catalog.na)}}</div>
			</div>
		</div>

		<div class="cat-cell-check">
			<div v-if='catalog.noq+catalog.autoq==0'><img
					src='https://upload.wikimedia.org/wikipedia/commons/thumb/f/fb/Yes_check.svg/20px-Yes_check.svg.png' />
			</div>
		</div>

	</div>
`
};
