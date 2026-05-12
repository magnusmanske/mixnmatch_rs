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
            // Monotonic request token. If the user presses 's'/'n' twice in
            // quick succession, two loadData calls run concurrently and can
            // resolve out of order — without this, an older response can
            // overwrite a newer one. After each await, check that this call
            // is still the latest; if not, bail silently (the newer call owns
            // loaded/entry/catalog/error display).
            let token = me._loadToken = (me._loadToken || 0) + 1;
            me.loaded = false;
            try {
                let d = await mnm_api('random', { catalog: (me.id || 0), submode: me.submode });
                if (token !== me._loadToken) return;
                me.entry = d.data;
                await ensure_catalog(me.entry.catalog);
                if (token !== me._loadToken) return;
                me.catalog = get_specific_catalog(me.entry.catalog) || {};
                me.loaded = true;
            } catch (e) {
                if (token !== me._loadToken) return;
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
			<!--
				Force fresh instances of every child that does async work whenever
				the entry changes. Without the key Vue reuses the same instance and
				only swaps the prop — pending fetches from the previous entry can
				then complete after the swap and overwrite results for the current
				entry. The key destroys the old instance so its in-flight writes
				land on detached reactive data and never reach the DOM. Keying
				entry-details also re-mounts its descendant catalog-entry-multi-match.
			-->
			<entry-details :entry='entry' :key='entry.id' :random="1" v-on:random_entry_button_clicked="loadData"
				:show_catalog='typeof id=="undefined"'></entry-details>
			<match-entry :entry='entry' :key='entry.id'></match-entry>
		</div>
		<div v-else>
			<i tt="loading"></i>
		</div>

	</div>
`
});
