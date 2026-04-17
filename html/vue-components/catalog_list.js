import { mnm_api, mnm_notify, mnm_loading, ensure_catalog, get_specific_catalog, tt_update_interface, wd } from './store.js';

export default Vue.extend({
	props: ['id', 'mode', 'start'],
	data: function () {
		return {
			entries: {}, catalog: {}, loaded: false, per_page: 50,
			user_filter: '', keyword_filter: '', type_filter: '',
			total_filtered: 0,
			mode2prop: { manual: 'manual', auto: 'autoq', unmatched: 'unmatched', nowd: 'nowd', na: 'na', multi_match: 'multi_match', type: undefined, title_match: undefined },
			modes: {
				manual: { show_noq: 0, show_autoq: 0, show_userq: 1, show_na: 0, show_multiple: 0 },
				auto: { show_noq: 0, show_autoq: 1, show_userq: 0, show_na: 0, show_multiple: 0 },
				unmatched: { show_noq: 1, show_autoq: 0, show_userq: 0, show_na: 0, show_multiple: 0 },
				nowd: { show_noq: 0, show_autoq: 0, show_userq: 0, show_na: 0, show_nowd: 1, show_multiple: 0 },
				na: { show_noq: 0, show_autoq: 0, show_userq: 0, show_na: 1, show_multiple: 0 },
				multi_match: { show_noq: 0, show_autoq: 0, show_userq: 0, show_na: 0, show_multiple: 1 },
			}
		}
	},
	created: function () {
		//		console.log ( this.id,this.mode);
		this.loadCatalogList(this.id, this.mode);
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		loadCatalogList: async function (id, mode) {
			const me = this;
			me.type_filter = me.$route.query.type || '';
			me.title_match = me.$route.query.match;
			me.keyword_filter = me.$route.query.filter || '';
			if (me.$route.query.user_id) me.user_filter = me.$route.query.user_id;
			me.loaded = false;
			mnm_loading(true);
			try {
				if (typeof me.start == 'undefined') me.start = 0;
				var meta = Object.assign({ offset: me.start * me.per_page, per_page: me.per_page }, me.modes[mode]);
				let params = { catalog: id, meta: JSON.stringify(meta) };
				if (me.type_filter !== '') params.type = me.type_filter;
				if (typeof me.title_match != 'undefined') params.title_match = me.title_match;
				if (me.keyword_filter !== '') params.keyword = me.keyword_filter;
				if (me.user_filter !== '') params.user_id = me.user_filter;
				var d = await mnm_api('catalog', params);
				me.total_filtered = d.total_filtered || 0;
				let qs = [];
				Object.entries(d.data.entries).forEach(function ([k, v]) {
					if (typeof v.q != 'undefined' && v.q > 0) qs.push('Q' + v.q);
					(v.multimatch || []).forEach(function (q) { qs.push(q) });
					if (typeof d.data.users[v.user] == 'undefined') return;
					d.data.entries[k].username = d.data.users[v.user].name;
				});
				await ensure_catalog(id);
				me.catalog = get_specific_catalog(id) || {};
				(me.catalog.types || '').split(/[,|]/).forEach(function (q) { if (q) qs.push(q) });
				wd.getItemBatch(qs).then(function () {
					me.entries = d.data.entries;
					me.loaded = true;
					mnm_loading(false);
				});
			} catch (e) {
				mnm_loading(false);
				mnm_notify('Failed to load catalog list: ' + e.message, 'danger');
				me.loaded = true;
			}
		},
		buildQuery: function () {
			let me = this;
			let query = {};
			if (me.type_filter !== '') query.type = me.type_filter;
			if (me.title_match) query.match = me.title_match;
			if (me.keyword_filter !== '') query.filter = me.keyword_filter;
			if (me.user_filter !== '') query.user_id = me.user_filter;
			return query;
		},
		navigateToPage0: function () {
			this.$router.push({ path: '/list/' + this.id + '/' + (this.mode || '') + '/0', query: this.buildQuery() });
		},
		applyUserFilter: function () { this.navigateToPage0(); },
		clearUserFilter: function () { this.user_filter = ''; this.navigateToPage0(); },
		applyKeywordFilter: function () { this.navigateToPage0(); },
		clearKeywordFilter: function () { this.keyword_filter = ''; this.navigateToPage0(); },
		applyTypeFilter: function () { this.navigateToPage0(); },
		clearTypeFilter: function () { this.type_filter = ''; this.navigateToPage0(); },
		goToPage: function (offset) {
			let page = Math.floor(offset / this.per_page);
			this.$router.push({ path: '/list/' + this.id + '/' + (this.mode || '') + '/' + page, query: this.buildQuery() });
		},
		getTypeLabel: function (t) {
			if (!t || t === '') return '';
			if (!/^Q\d+$/.test(t)) return t;
			var item = wd.getItem(t);
			if (item) return item.getLabel() + ' [' + t + ']';
			wd.getItemBatch([t]).then(function () {
				var el = document.querySelector('.mnm-type-select option[value="' + t + '"]');
				if (!el) return;
				var item2 = wd.getItem(t);
				if (item2) el.textContent = item2.getLabel() + ' [' + t + ']';
			});
			return t;
		}
	},
	computed: {
		catalog_types: function () {
			var types = (this.catalog && this.catalog.types) || '';
			return types.indexOf('|') !== -1 ? types.split('|') : [];
		},
		nav_total: function () {
			if (this.total_filtered > 0) return this.total_filtered;
			return (this.catalog[this.mode2prop[this.mode]] || 0) * 1;
		},
		nav_offset: function () {
			return (this.start || 0) * this.per_page;
		}
	},
	watch: {
		'$route'(to, from) {
			this.loadCatalogList(to.params.id, to.params.mode);
		}
	},
	template: `
	<div>
		<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
			{text: catalog.name, to: '/catalog/'+catalog.id},
			{text: mode}
		]"></mnm-breadcrumb>
		<catalog-header :catalog="catalog"></catalog-header>
		<div v-if='loaded'>
			<div class='d-flex flex-wrap align-items-center gap-2 mb-2'>
				<div class='d-flex align-items-center gap-1'>
					<input type='text' class='form-control form-control-sm' style='width:14em'
						v-model='keyword_filter' placeholder='Filter by keyword\u2026'
						@keyup.enter='applyKeywordFilter' />
					<button class='btn btn-outline-primary btn-sm' @click.prevent='applyKeywordFilter'>Filter</button>
					<button v-if='keyword_filter!==""' class='btn btn-outline-secondary btn-sm' @click.prevent='clearKeywordFilter'>&times;</button>
				</div>
				<div v-if='catalog_types.length > 0' class='d-flex align-items-center gap-1'>
					<select class='form-select form-select-sm mnm-type-select' style='width:auto;min-width:140px'
						v-model='type_filter' @change='applyTypeFilter'>
						<option value='' tt='all_types'></option>
						<option v-for='t in catalog_types' :value='t'>{{getTypeLabel(t)}}</option>
					</select>
					<button v-if='type_filter!==""' class='btn btn-outline-secondary btn-sm' @click.prevent='clearTypeFilter'>&times;</button>
				</div>
				<div v-if='mode=="manual"' class='d-flex align-items-center gap-1'>
					<select class='form-select form-select-sm' style='width:auto;min-width:180px' v-model='user_filter' @change='applyUserFilter'>
						<option value='' tt='filter_by_user'></option>
						<option value='0'>Automatchers (user=0)</option>
						<option value='3'>Name/date matcher</option>
						<option value='4'>Auxiliary data matcher</option>
					</select>
					<button v-if='user_filter!==""' class='btn btn-outline-secondary btn-sm' @click.prevent='clearUserFilter'>&times;</button>
				</div>
				</div>
			<pagination v-if="nav_total > per_page" :offset="nav_offset" :items-per-page="per_page" :total="nav_total"
				:show-first-last="true" @go-to-page="goToPage"></pagination>
			<div v-if='Object.keys(entries).length > 0'>
				<entry-list-item v-for="e in entries" :entry="e" :show_permalink="1" :key="e.id"></entry-list-item>
			</div>
			<div v-else class='mnm-empty-state'>
				<div class='mnm-empty-icon'>&#x1F4AD;</div>
				<p tt='no_entries_in_this_view'></p>
			</div>
			<pagination v-if="nav_total > per_page" :offset="nav_offset" :items-per-page="per_page" :total="nav_total"
				@go-to-page="goToPage"></pagination>
		</div>
		<div v-else>
			<i tt="loading"></i>
		</div>
	</div>
`
});
