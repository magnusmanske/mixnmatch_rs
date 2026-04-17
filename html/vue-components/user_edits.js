import { mnm_api, mnm_notify, mnm_loading, ensure_catalog, get_specific_catalog, tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['user_id', 'catalog_id'],
	data: function () {
		return {
			events: {}, user_info: null, catalog: null,
			loaded: false, loading_more: false,
			offset: 0, per_page: 50, total: 0
		};
	},
	created: function () {
		this.loadData();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	computed: {
		has_more: function () {
			return this.offset + this.per_page < this.total;
		},
		user_display_name: function () {
			if (!this.user_info) return 'User #' + this.user_id;
			return this.user_info.name || ('User #' + this.user_id);
		},
		is_system_user: function () {
			var id = this.user_id * 1;
			return id === 0 || id === 3 || id === 4;
		},
		system_user_label: function () {
			var id = this.user_id * 1;
			if (id === 0) return 'Automatic, preliminary matcher';
			if (id === 3) return 'Automatic name/date matcher';
			if (id === 4) return 'Auxiliary data matcher';
			return '';
		}
	},
	methods: {
		loadData: async function () {
			let me = this;
			me.loaded = false;
			me.offset = 0;
			me.events = {};
			mnm_loading(true);
			try {
				if (me.catalog_id) {
					await ensure_catalog(me.catalog_id);
					me.catalog = get_specific_catalog(me.catalog_id) || null;
				}
				let d = await mnm_api('user_edits', {
					user_id: me.user_id,
					catalog: me.catalog_id || 0,
					limit: me.per_page,
					offset: 0
				});
				me.events = d.data.events || {};
				me.user_info = d.data.user_info;
				me.total = d.total || 0;
				// Attach usernames
				Object.entries(me.events).forEach(function ([k, v]) {
					if (d.data.users[v.user]) me.events[k].username = d.data.users[v.user].name;
				});
				me.loaded = true;
			} catch (e) {
				mnm_notify('Failed to load user edits: ' + e.message, 'danger');
				me.loaded = true;
			}
			mnm_loading(false);
		},
		loadMore: async function () {
			let me = this;
			me.loading_more = true;
			me.offset += me.per_page;
			try {
				let d = await mnm_api('user_edits', {
					user_id: me.user_id,
					catalog: me.catalog_id || 0,
					limit: me.per_page,
					offset: me.offset
				});
				Object.entries(d.data.events || {}).forEach(function ([k, v]) {
					if (d.data.users[v.user]) v.username = d.data.users[v.user].name;
					Vue.set(me.events, k, v);
				});
			} catch (e) {
				mnm_notify('Failed to load more: ' + e.message, 'danger');
				me.offset -= me.per_page;
			}
			me.loading_more = false;
		},
		get_catalog: function (catalog_id) {
			return get_specific_catalog(catalog_id);
		}
	},
	watch: {
		'$route'(to) {
			this.loadData();
		}
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{text: 'User edits'}]"></mnm-breadcrumb>
	<div v-if='!loaded' class='text-center py-4'><i tt='loading'></i></div>
	<div v-else>
		<!-- Header -->
		<div class='mb-3'>
			<h4 class='mb-1'>
				<span v-if='is_system_user'>{{system_user_label}}</span>
				<span v-else-if='user_info'>{{user_info.name}}</span>
				<span v-else>User #{{user_id}}</span>
			</h4>
			<div v-if='user_info && !is_system_user' class='mb-2'>
				<a class='btn btn-outline-secondary btn-sm wikidata' target='_blank'
					:href="'https://www.wikidata.org/wiki/User:'+encodeURIComponent((user_info.name||'').replace(/ /g,'_'))">
					Wikidata user page</a>
				<a class='btn btn-outline-secondary btn-sm wikidata ms-1' target='_blank'
					:href="'https://www.wikidata.org/wiki/Special:Contributions/'+encodeURIComponent((user_info.name||'').replace(/ /g,'_'))">
					Contributions</a>
			</div>
			<div class='text-muted small'>
				<span v-if='catalog'>
					<span tt='edits_in_catalog'></span>
					<router-link :to="'/catalog/'+catalog_id">{{catalog.name}}</router-link>
					&mdash; <router-link :to="'/user/'+user_id">all catalogs</router-link>
				</span>
				<span v-else tt='edits_across_all_catalogs'></span>
				&mdash; {{total}} edits
			</div>
		</div>

		<!-- Events -->
		<div v-if='Object.keys(events).length === 0' class='mnm-empty-state'>
			<p tt='no_results'></p>
		</div>
		<div v-else>
			<entry-list-item v-for='(e, k) in events' :entry='e' :rc='true'
				:show_catalog='!catalog_id' :show_permalink='true' :key='k'></entry-list-item>
		</div>

		<!-- Load more -->
		<div v-if='has_more' class='text-center py-3'>
			<button class='btn btn-outline-primary' @click.prevent='loadMore' :disabled='loading_more'>
				<span v-if='loading_more' class='spinner-border spinner-border-sm me-1' role='status'></span>
				<span tt='load_more'></span>
			</button>
		</div>
	</div>
</div>
`
});
