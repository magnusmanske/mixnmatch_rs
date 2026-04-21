(function() {
  const s = document.createElement('style');
  s.textContent = `
.cat-preview { padding: 8px 10px; border-bottom: 1px solid var(--mnm-border, #dee2e6); }
.cat-preview:nth-child(odd) { background: var(--mnm-bg-alt, #f4f6f8); }
.cat-preview-name { font-weight: 600; font-size: 0.95rem; }
.cat-preview-name a { color: var(--mnm-blue, #36c); }
.cat-preview-name a:hover { text-decoration: underline; }
.cat-preview-desc { font-size: 0.8rem; color: var(--mnm-text-muted, #6c757d); line-height: 1.3; margin-top: 1px; }
.cat-preview-meta { font-size: 0.75rem; color: var(--mnm-text-light, #767676); margin-top: 1px; }
`;
  document.head.appendChild(s);
})();

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
