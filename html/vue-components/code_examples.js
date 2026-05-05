import { mnm_api, tt_update_interface } from './store.js';

(function () {
    const s = document.createElement('style');
    s.textContent = `
#ce-table .ce-lua  { background:#00007f; color:#fff; }
#ce-table .ce-php  { background:#777bb3; color:#fff; }
#ce-table .ce-lang {
    display:inline-block;
    font-size:0.7rem;
    font-weight:bold;
    padding:1px 5px;
    border-radius:3px;
    margin-right:2px;
}
#ce-table .ce-code {
    font-family: monospace;
    font-size: 0.78rem;
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 6rem;
    overflow: hidden;
    color: inherit;
    background: transparent;
    border: none;
    padding: 0;
    margin: 0;
    display: block;
}
#ce-table td { vertical-align: top; }
`;
    document.head.appendChild(s);
})();

export default Vue.extend({
    props: ['function_filter'],
    data: function () {
        return {
            rows: [],
            total: 0,
            all_functions: [],
            loaded: false,
            per_page: 50,
            start: 0,
            // local copy of the filter so the dropdown doesn't mutate the prop
            selected_function: this.function_filter || '',
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
            me.loaded = false;
            try {
                const d = await mnm_api('get_code_examples', {
                    function: me.selected_function,
                    start: me.start,
                    max: me.per_page,
                });
                me.rows = d.rows || [];
                me.total = d.total || 0;
                me.all_functions = d.all_functions || [];
            } finally {
                me.loaded = true;
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
        // Returns first N non-empty lines of a code string for the preview.
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
        <span v-if='loaded' class='text-muted ms-2' style='font-size:0.85rem'>
            {{total}} fragment<span v-if='total!==1'>s</span>
        </span>
    </div>

    <div v-if='!loaded' class='text-muted fst-italic' tt='loading'></div>

    <template v-if='loaded'>
        <pagination v-if='total > per_page'
            :offset='start' :items-per-page='per_page' :total='total'
            :show-first-last='true' @go-to-page='goToPage'></pagination>

        <div class='table-responsive'>
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
                        <span v-if='row.has_lua' class='ce-lang ce-lua'>Lua</span>
                        <span v-if='row.has_php' class='ce-lang ce-php'>PHP</span>
                    </td>
                    <td style='width:99%'>
                        <code class='ce-code' v-if='row.has_lua'>{{codePreview(row.lua)}}</code>
                        <span v-else class='text-muted fst-italic' style='font-size:0.8rem'>PHP only</span>
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
