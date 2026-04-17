import { entryDisplayMixin } from './mnm-mixins.js';
import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface } from './store.js';

export default Vue.extend({
    mixins: [entryDisplayMixin],
    props: ["id", "mode"],
    data: function () { return { catalog: {}, entry: {}, loaded: false, submode: 'unmatched' }; },
    created: function () {
        if (typeof this.mode != 'undefined' && this.mode != '') this.submode = this.mode;
        this.loadData();
        tt_update_interface();
    },
    updated: function () { tt_update_interface(); },
    mounted: function () {
        tt_update_interface();
        var me = this;
        me._keyHandler = function (e) {
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;
            if (e.key === 's' || e.key === 'n') { e.preventDefault(); me.loadData(); }
        };
        document.addEventListener('keydown', me._keyHandler);
    },
    beforeDestroy: function () {
        if (this._keyHandler) document.removeEventListener('keydown', this._keyHandler);
    },
    methods: {
        loadData: async function () {
            let me = this;
            me.loaded = false;
            try {
                let d = await mnm_api('random', { catalog: (me.id || 0), submode: me.submode });
                me.entry = d.data;
                await ensure_catalog(me.entry.catalog);
                me.catalog = get_specific_catalog(me.entry.catalog) || {};
                me.loaded = true;
            } catch (e) {
                mnm_notify(e.message || 'Failed to load random entry', 'danger');
                me.loaded = true;
            }
        },
        get_catalog(catalog_id) {
            return get_specific_catalog(catalog_id);
        }
    },
    template: `
	<div>
		<div v-if="loaded">
			<mnm-breadcrumb v-if='get_catalog(entry.catalog) && get_catalog(entry.catalog).id' :crumbs="[
				{text: get_catalog(entry.catalog).name, to: '/catalog/'+entry.catalog},
				{text: 'Random'}
			]"></mnm-breadcrumb>
			<catalog-header :catalog="get_catalog(entry.catalog)"></catalog-header>
			<entry-details :entry='entry' :random="1" v-on:random_entry_button_clicked="loadData"
				:show_catalog='typeof id=="undefined"'></entry-details>
			<match-entry :entry='entry'></match-entry>
		</div>
		<div v-else>
			<i tt="loading"></i>
		</div>

	</div>
`
});
