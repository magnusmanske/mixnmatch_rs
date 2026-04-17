import { mnm_api, mnm_notify, mnm_loading, ensure_catalogs, get_specific_catalog, tt_update_interface, widar } from './store.js';

(function () {
	const s = document.createElement('style');
	s.textContent = `.tg-search-results { max-height: 300px; overflow-y: auto; }
.tg-group-item { cursor: pointer; padding: 6px 12px; }
.tg-group-item:hover, .tg-group-item.active { background: #e9ecef; }
.tg-catalog-tag { display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px;
    background: #e9ecef; border-radius: 14px; font-size: 0.85rem; }
.tg-catalog-tag .tg-remove { cursor: pointer; color: #888; font-weight: bold; }
.tg-catalog-tag .tg-remove:hover { color: #c00; }
`;
	document.head.appendChild(s);
})();

export default Vue.extend({
	props: ['id'],
	data: function () {
		return {
			groups: [], search_query: '', filtered_groups: [],
			selected_group: null, original_catalog_ids: [],
			current_catalogs: [], new_group_name: '',
			loaded: false, loading_detail: false, saving: false,
			show_create: false
		};
	},
	created: async function () {
		let me = this;
		mnm_loading(true);
		let d = await mnm_api('get_top_groups');
		me.groups = d.data || [];
		me.loaded = true;
		mnm_loading(false);
		me.filtered_groups = me.groups;
		if (me.id) me.selectGroupById(me.id * 1);
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	computed: {
		has_changes: function () {
			if (!this.selected_group) return false;
			let orig = this.original_catalog_ids.map(String).sort().join(',');
			let curr = this.current_catalogs.map(function (c) { return String(c.id); }).sort().join(',');
			return orig !== curr;
		},
		changes_summary: function () {
			if (!this.selected_group) return '';
			let orig_set = {};
			this.original_catalog_ids.forEach(function (id) { orig_set[String(id)] = true; });
			let curr_set = {};
			this.current_catalogs.forEach(function (c) { curr_set[String(c.id)] = true; });
			let added = this.current_catalogs.filter(function (c) { return !orig_set[String(c.id)]; });
			let removed = this.original_catalog_ids.filter(function (id) { return !curr_set[String(id)]; });
			let parts = [];
			if (added.length > 0) parts.push('+' + added.length + ' added');
			if (removed.length > 0) parts.push('-' + removed.length + ' removed');
			return parts.join(', ');
		}
	},
	methods: {
		filterGroups: function () {
			let q = this.search_query.toLowerCase().trim();
			if (q === '') { this.filtered_groups = this.groups; return; }
			this.filtered_groups = this.groups.filter(function (g) {
				return g.name.toLowerCase().indexOf(q) >= 0;
			});
		},
		selectGroupById: function (id) {
			let me = this;
			let g = me.groups.find(function (g) { return g.id * 1 === id; });
			if (g) me.selectGroup(g);
		},
		selectGroup: async function (g) {
			let me = this;
			me.loading_detail = true;
			me.selected_group = g;
			let path = '/top_groups/' + g.id;
			if (me.$route.path !== path) me.$router.replace(path);

			mnm_loading(true);
			let catalog_ids = (g.catalogs || '').split(',').filter(function (c) { return c.trim() !== ''; });
			await ensure_catalogs(catalog_ids);
			let catalogs = [];
			catalog_ids.forEach(function (cid) {
				let cat = get_specific_catalog(cid * 1);
				if (cat) catalogs.push({ id: cid * 1, name: cat.name });
			});
			me.current_catalogs = catalogs;
			me.original_catalog_ids = catalogs.map(function (c) { return String(c.id); });
			me.loading_detail = false;
			mnm_loading(false);
		},
		backToList: function () {
			this.selected_group = null;
			this.current_catalogs = [];
			this.original_catalog_ids = [];
			if (this.$route.path !== '/top_groups') this.$router.replace('/top_groups');
		},
		onCatalogsChange: function (list) {
			this.current_catalogs = list;
		},
		removeCatalog: function (id) {
			this.current_catalogs = this.current_catalogs.filter(function (c) { return c.id * 1 !== id * 1; });
		},
		saveChanges: async function () {
			let me = this;
			me.saving = true;
			let cids = me.current_catalogs.map(function (c) { return c.id; }).join(',');
			try {
				await mnm_api('set_top_group', {
					group_id: me.selected_group.id,
					group_name: me.selected_group.name,
					catalogs: cids,
					username: widar.getUserName()
				}, { method: 'POST' });
				me.selected_group.catalogs = cids;
				me.original_catalog_ids = me.current_catalogs.map(function (c) { return String(c.id); });
				mnm_notify('Group saved', 'success');
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
			me.saving = false;
		},
		createNewGroup: async function () {
			let me = this;
			let name = (me.new_group_name || '').trim();
			if (name === '') {
				mnm_notify('Please enter a group name', 'warning');
				return;
			}
			try {
				await mnm_api('set_top_group', {
					group_id: 0,
					group_name: name,
					catalogs: '',
					username: widar.getUserName()
				}, { method: 'POST' });
				me.new_group_name = '';
				me.show_create = false;
				// Reload groups
				let d = await mnm_api('get_top_groups');
				me.groups = d.data || [];
				me.filterGroups();
				mnm_notify('Group "' + name + '" created', 'success');
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
		},
		deleteEmptyGroup: async function () {
			let me = this;
			if (me.current_catalogs.length > 0) {
				mnm_notify('Remove all catalogs first, then save before deleting', 'warning');
				return;
			}
			try {
				await mnm_api('remove_empty_top_group', {
					group_id: me.selected_group.id,
					username: widar.getUserName()
				}, { method: 'POST' });
				me.groups = me.groups.filter(function (g) { return g.id * 1 !== me.selected_group.id * 1; });
				me.filterGroups();
				me.selected_group = null;
				if (me.$route.path !== '/top_groups') me.$router.replace('/top_groups');
				mnm_notify('Group deleted', 'success');
			} catch (e) {
				mnm_notify(e.message, 'danger');
			}
		}
	},
	watch: {
		'$route'(to) {
			if (to.params.id && (!this.selected_group || this.selected_group.id != to.params.id)) {
				this.selectGroupById(to.params.id * 1);
			} else if (!to.params.id) {
				this.selected_group = null;
			}
		}
	},
	template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{tt: 'top_groups'}]"></mnm-breadcrumb>
	<h4 tt='top_groups'></h4>
	<p class='text-muted small' tt='top_groups_blurb'></p>

	<div v-if='!loaded' class='text-center py-4'><i tt='loading'></i></div>

	<!-- Group detail view -->
	<div v-else-if='selected_group'>
		<div class='d-flex align-items-center mb-3'>
			<a href='#' class='me-2' @click.prevent='backToList'>&larr;</a>
			<h5 class='mb-0'>{{selected_group.name}}</h5>
			<small class='text-muted ms-2'>
				<span tt='by'></span> <userlink :username='selected_group.user_name'></userlink>
			</small>
		</div>

		<div v-if='loading_detail' class='text-center py-3'><i tt='loading'></i></div>
		<div v-else>
			<!-- Catalog list: editable picker for logged-in users, read-only tags otherwise -->
			<div v-if='widar.is_logged_in' class='mb-3'>
				<catalog-search-picker :multi='true' :linkable='true' :value='current_catalogs' @change='onCatalogsChange'
					placeholder='Search catalogs to add...'></catalog-search-picker>
			</div>
			<div v-else class='mb-3'>
				<div class='d-flex flex-wrap gap-1'>
					<span v-for='c in current_catalogs' :key='c.id' class='tg-catalog-tag'>
						<router-link :to="'/catalog/'+c.id">{{c.name}}</router-link>
					</span>
					<span v-if='current_catalogs.length==0' class='text-muted small'>(empty group)</span>
				</div>
			</div>

			<div class='d-flex align-items-center gap-2 mb-3'>
				<router-link :to="'/top_missing/'+current_catalogs.map(c=>c.id).join(',')"
					class='btn btn-outline-primary btn-sm' v-if='current_catalogs.length>0' tt='show_top_missing'></router-link>
				<button v-if='has_changes' class='btn btn-outline-success btn-sm' @click.prevent='saveChanges' :disabled='saving'>
					<span v-if='saving' class='spinner-border spinner-border-sm me-1' role='status'></span>
					<span tt='save_changes'></span>
				</button>
				<small v-if='has_changes' class='text-muted'>{{changes_summary}}</small>
			</div>

			<div v-if='widar.is_logged_in && current_catalogs.length==0 && !has_changes' class='mt-3'>
				<button class='btn btn-outline-danger btn-sm' @click.prevent='deleteEmptyGroup' tt='remove_empty_group'></button>
			</div>
		</div>
	</div>

	<!-- Group list view -->
	<div v-else>
		<div class='mb-3'>
			<input type='text' class='form-control' v-model='search_query' @input='filterGroups'
				placeholder='Search groups...' />
		</div>

		<div v-if='filtered_groups.length==0 && search_query!==""' class='text-muted py-2' tt='no_results'></div>

		<div class='list-group mb-3'>
			<a v-for='g in filtered_groups' :key='g.id' href='#' class='list-group-item list-group-item-action d-flex justify-content-between align-items-center'
				@click.prevent='selectGroup(g)'>
				<div>
					<span class='fw-semibold'>{{g.name}}</span>
					<small class='text-muted ms-2'><userlink :username='g.user_name'></userlink></small>
				</div>
				<span class='badge bg-secondary rounded-pill'>{{(g.catalogs||'').split(',').filter(c=>c!=='').length}}</span>
			</a>
		</div>

		<div v-if='widar.is_logged_in'>
			<div v-if='!show_create'>
				<button class='btn btn-outline-primary btn-sm' @click.prevent='show_create=true' tt='create_new_group'></button>
			</div>
			<div v-else class='card p-3'>
				<div class='d-flex gap-2'>
					<input type='text' class='form-control form-control-sm' v-model='new_group_name'
						placeholder='New group name...' @keydown.enter='createNewGroup' />
					<button class='btn btn-primary btn-sm' @click.prevent='createNewGroup' tt='create'></button>
					<button class='btn btn-outline-secondary btn-sm' @click.prevent='show_create=false'>&times;</button>
				</div>
			</div>
		</div>
	</div>
</div>
`
});
