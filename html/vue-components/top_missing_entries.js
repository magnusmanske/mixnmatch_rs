import { editEntryMixin } from './mnm-mixins.js';
import { mnm_api, mnm_notify, mnm_loading, ensure_catalogs, get_specific_catalog, tt_update_interface } from './store.js';

(function () {
    var s = document.createElement('style');
    s.textContent = `
.tm-row { display: flex; align-items: baseline; gap: 0.5rem; padding: 0.35rem 0.5rem; border-radius: 4px; transition: background 0.15s; }
.tm-row:nth-child(odd) { background: var(--mnm-bg-alt, #f8f9fa); }
.tm-row:hover { background: #e2e6ea; }
.tm-row.tm-visited { background: #fff3cd; }
.tm-row.tm-visited:hover { background: #ffe69c; }
.tm-rank { min-width: 2rem; text-align: right; color: #6c757d; font-size: 0.82rem; }
.tm-name { flex: 1; }
.tm-cnt { white-space: nowrap; color: #6c757d; font-size: 0.85rem; }
.tm-bar-wrap { width: 60px; height: 6px; background: #e9ecef; border-radius: 3px; overflow: hidden; flex-shrink: 0; }
.tm-bar { height: 100%; background: var(--mnm-blue, #36c); border-radius: 3px; }
.tm-search-bar { display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center;
    padding: 0.5rem 0.75rem; border: 1px solid var(--mnm-border, #dee2e6);
    border-radius: 0.25rem; background: var(--mnm-bg-alt, #f4f6f8);
    margin-bottom: 0.75rem; }
.tm-search-bar .tm-spacer { flex: 1 1 auto; }
.tm-hint { font-size: 0.8125rem; color: var(--mnm-text-muted, #6c757d); }
.tm-stale { font-size: 0.8125rem; color: #9a7a00;
    padding: 0.4rem 0.6rem; border: 1px solid #ffe08a; background: #fff8d9;
    border-radius: 0.25rem; margin-bottom: 0.5rem;
    display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }
`;
    document.head.appendChild(s);
})();

export default Vue.extend({
    mixins: [editEntryMixin],
    props: ['catalogs'],
    data: function () {
        return {
            data: [], require_catalogs: [], selected_catalogs: [],
            require_catalogs_string: '', loading: false, has_loaded: false,
            loaded_for: '', // `require_catalogs_string` value at the time of the last successful load
            visited: {}, filter_text: ''
        };
    },
    created: async function () {
        let me = this;
        let catalog_ids = (me.catalogs || '').split(',').filter(c => c !== '');
        await ensure_catalogs(catalog_ids);
        catalog_ids.forEach(function (cid) {
            let cat = get_specific_catalog(cid * 1);
            if (cat) me.selected_catalogs.push({ id: cid * 1, name: cat.name });
        });
        me.require_catalogs_string = me.selected_catalogs.map(c => c.id).join(',');
        // Auto-run only when the user arrived with a valid multi-catalog URL.
        // Manual adds after that require the explicit Search button so adding
        // a catalog doesn't silently trigger a slow query each time.
        if (me.selected_catalogs.length >= 2) me.loadData();
        tt_update_interface();
    },
    updated: function () { tt_update_interface() },
    mounted: function () { tt_update_interface() },
    computed: {
        max_cnt: function () {
            var m = 0;
            this.data.forEach(function (d) { if (d.cnt > m) m = d.cnt; });
            return m;
        },
        filtered_data: function () {
            var q = this.filter_text.toLowerCase().trim();
            if (!q) return this.data;
            return this.data.filter(function (d) { return d.ext_name.toLowerCase().indexOf(q) !== -1; });
        },
        has_enough_catalogs: function () { return this.selected_catalogs.length >= 2; },
        selection_is_stale: function () {
            return this.has_loaded && this.has_enough_catalogs &&
                this.loaded_for !== this.require_catalogs_string;
        },
    },
    methods: {
        loadData: async function () {
            const me = this;
            if (!me.has_enough_catalogs) {
                me.require_catalogs = [];
                me.data = [];
                me.loaded_for = '';
                return;
            }
            me.require_catalogs = me.require_catalogs_string.split(',');
            me.loading = true;
            me.visited = {};
            me.filter_text = '';
            mnm_loading(true);
            try {
                var d = await mnm_api('top_missing', { catalogs: me.require_catalogs_string });
                me.data = d.data;
                me.loaded_for = me.require_catalogs_string;
            } catch (e) {
                mnm_notify(e.message || 'Failed to load data', 'danger');
            }
            me.has_loaded = true;
            me.loading = false;
            mnm_loading(false);
        },
        onCatalogsChange: function (list) {
            const me = this;
            me.selected_catalogs = list;
            me.require_catalogs_string = list.map(c => c.id).join(',');
            me.require_catalogs = list.map(c => '' + c.id);
            // Keep the URL in sync with the selection so the state can be
            // bookmarked and linked, but don't re-run the query — that
            // happens on explicit Search.
            me.updatePermalink();
            // If we've dropped below the minimum, clear the prior results so
            // the user isn't left with stale data and a confusing "stale"
            // banner that can never be refreshed.
            if (!me.has_enough_catalogs) {
                me.data = [];
                me.has_loaded = false;
                me.loaded_for = '';
            }
        },
        updatePermalink: function () {
            const me = this;
            var path = '/top_missing/' + me.require_catalogs_string;
            if (me.$route.path !== path) me.$router.replace(path);
        },
        markVisited: function (name) {
            Vue.set(this.visited, name, true);
        }
    },
    template: `
		<div>
			<mnm-breadcrumb :crumbs="[{text: 'Top missing entries'}]"></mnm-breadcrumb>
			<p tt='top_missing_blurb'></p>

			<div class='mb-2'>
				<label class='form-label' tt='used_catalogs'></label>
				<catalog-search-picker :multi="true" :value="selected_catalogs" @change="onCatalogsChange" placeholder="Search catalogs to add..."></catalog-search-picker>
			</div>

			<!-- Search toolbar + status hint -->
			<div class='tm-search-bar'>
				<button type='button' class='btn btn-primary btn-sm'
					:disabled='!has_enough_catalogs || loading'
					@click.prevent='loadData'>
					<span v-if='loading' class='spinner-border spinner-border-sm me-1' role='status' aria-hidden='true'></span>
					<span v-if='has_loaded && !selection_is_stale'>Re-run search</span>
					<span v-else>Search entries</span>
				</button>
				<span v-if='!has_enough_catalogs' class='tm-hint'>
					<span v-if='selected_catalogs.length === 0'>Add at least 2 catalogs to find entries whose names appear unmatched in more than one of them.</span>
					<span v-else>Add one more catalog — this tool compares names across catalogs, so at least 2 are required.</span>
				</span>
				<span v-else-if='!has_loaded' class='tm-hint'>
					Ready to search across {{selected_catalogs.length}} catalogs.
				</span>
				<span class='tm-spacer'></span>
				<span v-if='has_loaded && !selection_is_stale' class='tm-hint'>
					Last run: {{selected_catalogs.length}} catalogs &middot; {{data.length}} result{{data.length === 1 ? '' : 's'}}
				</span>
			</div>

			<div v-if='selection_is_stale' class='tm-stale'>
				<span aria-hidden='true'>&#x26A0;</span>
				<span>Catalog selection changed since the last search. Click <strong>Re-run search</strong> to refresh these results.</span>
			</div>

			<div v-if='loading' class='text-center py-3'><i tt='loading'></i></div>
			<div v-else-if='has_loaded && data.length === 0 && has_enough_catalogs'
				class='text-center py-3 text-muted'>
				<div tt='no_results'></div>
				<div class='small mt-1'>No shared names are unmatched across the selected catalogs. Try a different combination.</div>
			</div>
			<div v-else-if='data.length > 0'>
				<div class='d-flex flex-wrap align-items-center gap-2 mb-2'>
					<div class='d-flex align-items-center gap-1'>
						<input type='text' class='form-control form-control-sm' style='width:14em'
							v-model='filter_text' placeholder='Filter names\u2026' />
						<button v-if='filter_text!==""' class='btn btn-outline-secondary btn-sm' @click.prevent='filter_text=""'>&times;</button>
					</div>
					<small class='text-muted'>{{filtered_data.length}} <span v-if='filter_text'>of {{data.length}} </span>entries</small>
				</div>

				<div>
					<div v-for='(d, idx) in filtered_data' :key='d.ext_name'
						class='tm-row' :class='{ "tm-visited": visited[d.ext_name] }'>
						<span class='tm-rank'>{{idx + 1}}</span>
						<span class='tm-name'>
							<router-link :to="'/creation_candidates/by_ext_name/?ext_name='+encodeURIComponent(d.ext_name)"
								target='_blank' @click.native='markVisited(d.ext_name)'>{{d.ext_name}}</router-link>
						</span>
						<span class='tm-cnt'>{{d.cnt}}</span>
						<span class='tm-bar-wrap' :title='d.cnt + " catalogs"'>
							<span class='tm-bar' :style='"width:" + (d.cnt / max_cnt * 100) + "%"'></span>
						</span>
					</div>
				</div>
			</div>
		</div>
	`
});
