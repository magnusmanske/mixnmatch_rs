export default {
	name: 'catalog-preview',
	props: {
		catalog: { type: Object, required: true },
		link_to: { type: String, default: '' }
	},
	template: `
<div class='cat-preview'>
    <div class='cat-preview-name'>
        <router-link v-if='link_to' :to='link_to'>{{catalog.name}}</router-link>
        <span v-else>{{catalog.name}}</span>
    </div>
    <div v-if='catalog.desc' class='cat-preview-desc'>{{catalog.desc}}</div>
    <div class='cat-preview-meta'>
        <span v-if='catalog.type'>{{catalog.type}}</span>
        <span v-if='catalog.total*1>0'> &middot; {{Number(catalog.total).toLocaleString()}} entries</span>
        <span v-if='catalog.total*1>0'> &middot; {{Math.round(100*catalog.manual/catalog.total)}}% matched</span>
        <span v-if='catalog.wd_prop*1>0'> &middot; <wd-link :item='"P"+catalog.wd_prop'></wd-link></span>
        <span> &middot; #{{catalog.id}}</span>
    </div>
</div>
`
};
