import { mnm_api, tt_update_interface } from './store.js';


export default Vue.extend({
    props: ['function_filter'],
    data: function () {
        return {
            rows: [],
            total: 0,
            all_functions: [],
            loaded: false,
            loading: false,
            per_page: 50,
            start: 0,
            selected_function: this.function_filter || '',
            row_langs: {},   // row.id -> 'lua' | 'php'
            copied_id: null, // id of the row whose code was just copied
        };
    },
    watch: {
        // Respond to back/forward navigation that changes the prop
        function_filter: function (val) {
            this.selected_function = val || '';
            this.start = 0;
            this.load();
        },
    },
    created: async function () {
        await this.load();
    },
    updated: function () { tt_update_interface(); },
    mounted: function () { tt_update_interface(); },
    methods: {
        load: async function () {
            const me = this;
            me.loading = true;
            try {
                const d = await mnm_api('get_code_examples', {
                    function: me.selected_function,
                    start: me.start,
                    max: me.per_page,
                });
                const rows = d.data.rows || [];
                const langs = {};
                rows.forEach(function (r) { langs[r.id] = r.has_lua ? 'lua' : 'php'; });
                me.rows = rows;
                me.row_langs = langs;
                me.total = d.data.total || 0;
                me.all_functions = d.data.all_functions || [];
            } finally {
                me.loaded = true;
                me.loading = false;
            }
        },
        applyFilter: function () {
            const me = this;
            me.start = 0;
            const path = me.selected_function
                ? '/code_examples/' + encodeURIComponent(me.selected_function)
                : '/code_examples';
            me.$router.push(path);
        },
        clearFilter: function () {
            this.selected_function = '';
            this.applyFilter();
        },
        goToPage: function (offset) {
            this.start = offset;
            this.load();
            window.scrollTo(0, 0);
        },
        rowLang: function (row) {
            return this.row_langs[row.id] || (row.has_lua ? 'lua' : 'php');
        },
        setRowLang: function (row, lang) {
            Vue.set(this.row_langs, row.id, lang);
        },
        rowCode: function (row) {
            return this.rowLang(row) === 'lua' ? (row.lua || '') : (row.php || '');
        },
        copyCode: function (row) {
            var me = this;
            var code = me.rowCode(row);
            if (!code) return;
            navigator.clipboard.writeText(code).then(function () {
                me.copied_id = row.id;
                setTimeout(function () { if (me.copied_id === row.id) me.copied_id = null; }, 1500);
            });
        },
        codePreview: function (code) {
            if (!code) return '';
            return code
                .split('\n')
                .filter(function (l) { return l.trim().length > 0; })
                .slice(0, 5)
                .join('\n');
        },
    },
    template: `
<div class='mt-2'>
    <mnm-breadcrumb :crumbs="[{text: 'Code examples'}]"></mnm-breadcrumb>
    <h2>Code examples</h2>

    <!-- Filter bar -->
    <div class='d-flex align-items-center gap-2 mb-3 flex-wrap'>
        <label class='form-label mb-0 me-1' style='white-space:nowrap'>Function:</label>
        <select class='form-select form-select-sm' style='width:auto;min-width:180px'
            v-model='selected_function' @change='applyFilter'>
            <option value=''>All functions</option>
            <option v-for='fn in all_functions' :key='fn' :value='fn'>{{fn.replace(/_/g,' ')}}</option>
        </select>
        <button v-if='selected_function' class='btn btn-outline-secondary btn-sm'
            @click.prevent='clearFilter' title='Clear filter'>&times;</button>
        <span v-if='loaded && !loading' class='text-muted ms-2' style='font-size:0.85rem'>
            {{total}} fragment<span v-if='total!==1'>s</span>
        </span>
        <span v-if='loading' tt='loading' class='ms-2'></span>
    </div>

    <div v-if='!loaded' tt='loading'></div>

    <template v-if='loaded'>
        <pagination v-if='total > per_page'
            :offset='start' :items-per-page='per_page' :total='total'
            :show-first-last='true' @go-to-page='goToPage'></pagination>

        <div class='table-responsive' :style='loading ? "opacity:0.45;pointer-events:none" : ""'>
        <table class='table table-sm table-hover' id='ce-table' style='font-size:0.875rem'>
            <thead>
                <tr>
                    <th style='white-space:nowrap'>Function</th>
                    <th>Catalog</th>
                    <th>Lang</th>
                    <th>Code preview</th>
                    <th>Last run</th>
                </tr>
            </thead>
            <tbody>
                <tr v-if='rows.length === 0'>
                    <td colspan='5' class='text-muted fst-italic text-center'>No fragments found.</td>
                </tr>
                <tr v-for='row in rows' :key='row.id'>
                    <td style='white-space:nowrap'>
                        <router-link :to='"/code_examples/" + row.function'>
                            {{row.function.replace(/_/g,' ')}}
                        </router-link>
                    </td>
                    <td style='white-space:nowrap'>
                        <router-link :to='"/code/"+row.catalog'>
                            <span v-if='row.catalog_name'>{{row.catalog_name}}</span>
                            <span v-else class='text-muted'>#{{row.catalog}}</span>
                        </router-link>
                    </td>
                    <td style='white-space:nowrap'>
                        <button v-if='row.has_lua' class='ce-lang ce-lua'
                            :style='rowLang(row)==="lua" ? "" : "opacity:0.35"'
                            @click.prevent='setRowLang(row, "lua")'>Lua</button>
                        <button v-if='row.has_php' class='ce-lang ce-php'
                            :style='rowLang(row)==="php" ? "" : "opacity:0.35"'
                            @click.prevent='setRowLang(row, "php")'>PHP</button>
                    </td>
                    <td style='width:99%'>
                        <div class='ce-code-wrap'>
                            <code class='ce-code' v-if='rowCode(row)'>{{codePreview(rowCode(row))}}</code>
                            <span v-else class='text-muted fst-italic' style='font-size:0.8rem'>No code</span>
                            <button v-if='rowCode(row)' class='ce-copy-btn'
                                @click.prevent='copyCode(row)'
                                :title='copied_id===row.id ? "Copied!" : "Copy code"'>
                                <span v-if='copied_id===row.id'>&#10003;</span>
                                <span v-else>&#10696;</span>
                            </button>
                        </div>
                    </td>
                    <td style='white-space:nowrap;font-size:0.75rem;color:#6c757d'>
                        {{row.last_run ? row.last_run.substr(0,10) : ''}}
                    </td>
                </tr>
            </tbody>
        </table>
        </div>

        <pagination v-if='total > per_page'
            :offset='start' :items-per-page='per_page' :total='total'
            @go-to-page='goToPage'></pagination>
    </template>
</div>
`
});
