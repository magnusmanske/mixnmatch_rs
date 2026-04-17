import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

(function () {
    const s = document.createElement('style');
    s.textContent = `
form.test_entry_form > div {
    margin-right:0.2rem;
}
`;
    document.head.appendChild(s);
})();

export default Vue.extend({
    props: ['catalog_id'],
    data: function () {
        return {
            code_fragments: [], all_functions: [], available: [], loaded: false,
            is_user_allowed: false, error: '', test_entry_external_id: '', test_entry: {}
        };
    },
    created: async function () {
        const me = this;
        await ensure_catalog(me.catalog_id);
        me.catalog = get_specific_catalog(me.catalog_id);
        if (typeof me.catalog == 'undefined') me.error = 'Catalog is not defined';
        else me.loadCode();
    },
    updated: function () { tt_update_interface() },
    mounted: function () { tt_update_interface() },
    methods: {
        addCodeFragment: function (fn) {
            let me = this;
            let cf = { 'function': fn, catalog: me.catalog_id, php: '', lua: '', json: '', is_active: true, note: '' };
            me.code_fragments.push(cf);
            me.updateAvailable();
        },
        updateAvailable: function () {
            let me = this;
            me.available = [];
            me.all_functions.forEach(function (fn) {
                let found = false;
                me.code_fragments.forEach(function (cf) {
                    if (cf.function == fn) found = true;
                });
                if (!found) me.available.push(fn);
            });
        },
        useTestEntry: async function () {
            let me = this;
            let new_ext_id = (me.test_entry_external_id || '').trim();
            if (me.test_entry.ext_id == new_ext_id) return;
            try {
                let d = await mnm_api('get_entry', {
                    catalog: me.catalog_id,
                    ext_ids: JSON.stringify([new_ext_id])
                });
                me.test_entry = {};
                Object.values(d.data.entries).forEach(function (entry) {
                    me.test_entry = entry;
                });
                me.test_entry_external_id = me.test_entry.ext_id;
            } catch (e) {
                mnm_notify(e.message || 'Request failed', 'danger');
            }
        },
        loadRandomEntry: async function () {
            let me = this;
            let d = await mnm_api('random', {
                catalog: me.catalog_id
            });
            me.test_entry = d.data;
            me.test_entry_external_id = me.test_entry.ext_id;
        },
        loadCode: async function () {
            let me = this;
            if (!widar.loaded) {
                return setTimeout(me.loadCode, 100);
            }
            let d = await mnm_api('get_code_fragments', {
                catalog: me.catalog_id,
                username: widar.getUserName(),
            });
            me.code_fragments = d.data.fragments;
            me.all_functions = d.data.all_functions;
            me.updateAvailable();
            me.is_user_allowed = d.data.user_allowed == 1;
            me.loaded = true;
        }
    },
    template: `
<div class='mt-2'>
    <mnm-breadcrumb v-if='typeof catalog!="undefined" && catalog.id' :crumbs="[
        {text: catalog.name, to: '/catalog/'+catalog.id},
        {text: 'Code'}
    ]"></mnm-breadcrumb>
    <catalog-header v-if='typeof catalog!="undefined"' :catalog="catalog"></catalog-header>
	<div v-if='loaded'>

        <div class='card'>
            <div class="card-body">
                <h5 class="card-title" tt='test_entry'></h5>
                <div class="card-text">
                    <div style='display:flex;flex-direction:row;'>
                        <form class='d-flex test_entry_form'>
                            <div>
                                <input v-model='test_entry_external_id' tt_placeholder='ph_test_entry_external_id' />
                            </div>
                            <div>
                                <button class='btn btn-outline-primary' tt='use_test_entry' @click.prevent='useTestEntry()'></button>
                            </div>
                            <div>
                                <button class='btn btn-outline-secondary' tt='load_random_test_entry' @click.prevent='loadRandomEntry()'></button>
                            </div>
                        </form>
                    </div>
                </div>

                <div v-if='typeof test_entry.id!="undefined"'>
                    <entry-details :entry='test_entry'></entry-details>
                </div>
            </div>
        </div>

        <div v-for='(cf,cf_num) in code_fragments'>
            <code-fragment :fragment='cf' :key='"code_fragment_"+catalog_id+"_"+cf.function' :test_entry='test_entry' :is_user_allowed='is_user_allowed'></code-fragment>
        </div>

        <div v-for='fn in available' style='margin-top:0.2rem;'>
            <button class='btn btn-outline-primary' @click.prevent='addCodeFragment(fn)'>
                <span tt='add_code_fragment'></span>
                <span>{{fn.replace(/_/g,' ')}}</span>
            </button>
        </div>
    </div>
    <div v-else-if="error!=''" class="alert alert-danger" role="alert">
        {{error}}
    </div>
    <div v-else>
        <i tt='loading'></i>
    </div>
</div>
`
});
