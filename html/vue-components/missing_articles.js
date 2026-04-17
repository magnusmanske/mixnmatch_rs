import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface } from './store.js';

export const MissingArticles = Vue.extend({
  props: ["id", "site", "start"],
  data: function () { return { catalog: {}, entry_groups: [], loaded: false, page: 0, total: 0, last_key: '' } },
  created: function () { this.loadData(); },
  updated: function () { tt_update_interface(); var el = document.querySelector('.next_cc_set'); if (el) el.focus(); },
  mounted: function () { tt_update_interface() },
  methods: {
    loadData: async function () {
      const me = this;
      me.loaded = false;
      await ensure_catalog(me.id);
      me.catalog = get_specific_catalog(me.id);
      if (typeof me.start == 'undefined') me.page = 0;
      else me.page = me.start;

      var key = me.id + ':' + me.site;
      if (key == me.last_key) {
        me.loaded = true;
        return;
      }
      me.last_key = key;

      try {
        let d = await mnm_api('missingpages', { catalog: me.id, site: me.site });
        Object.entries(d.data.entries).forEach(function ([k, v]) {
          if (typeof d.data.users[v.user] == 'undefined') return;
          d.data.entries[k].username = d.data.users[v.user].name;
        });
        var eg = [[]];
        var last = 0;
        me.total = 0;
        Object.entries(d.data.entries).forEach(function ([k, v]) {
          if (eg[last].length >= 50) {
            eg.push([]);
            last++;
          }
          eg[last].push(v);
          me.total++;
        });
        me.entry_groups = eg;
      } catch (e) {
        mnm_notify(e.message || 'Request failed', 'danger');
      }
      me.loaded = true;
    },
    goToPage: function (offset) {
      let page = Math.floor(offset / 50);
      this.$router.push({ path: '/missing_articles/' + this.id + '/' + this.site + '/' + page });
    }
  },
  watch: {
    '$route'(to, from) {
      this.loadData();
    }
  },
  template: `<div>
	<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{text: 'Missing articles: '+site}
	]"></mnm-breadcrumb>
	<catalog-header :catalog="catalog"></catalog-header>
	<h2><span tt='missing_articles_on'></span> {{site}}</h2>
	<div v-if='loaded'>
		<pagination v-if="total > 50" :offset="page*50" :items-per-page="50" :total="total"
			:show-first-last="true" @go-to-page="goToPage"></pagination>
		<div><entry-list-item v-for="e in entry_groups[page]" :entry="e" :show_permalink="1"
				:key="e.id"></entry-list-item></div>
		<pagination v-if="total > 50" :offset="page*50" :items-per-page="50" :total="total"
			@go-to-page="goToPage"></pagination>
	</div>
	<div v-else>
		<i tt="loading"></i>
	</div>
</div>`
});

export default Vue.extend({
  props: ["id"],
  data: function () { return { catalog: {}, sites: [], loaded: false } },
  created: function () { this.loadData(); },
  updated: function () { tt_update_interface(); var el = document.querySelector('.next_cc_set'); if (el) el.focus(); },
  mounted: function () { tt_update_interface() },
  methods: {
    loadData: async function () {
      const me = this;
      me.loaded = false;
      await ensure_catalog(me.id);
      me.catalog = get_specific_catalog(me.id);
      let d = await mnm_api('sitestats', { catalog: me.id });
      var sites = [];
      Object.entries(d.data).forEach(function ([k, v]) {
        sites.push({ site: k, articles: v[me.id] });
      });
      me.sites = sites;
      me.loaded = true;
    }
  },
  watch: {
    '$route'(to, from) {
      this.loadData();
    }
  },
  template: `<div style='margin-top:20px'>
	<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{tt: 'site_stats'}
	]"></mnm-breadcrumb>
	<catalog-header :catalog="catalog"></catalog-header>
	<h2 tt='site_stats'></h2>
	<div v-if="loaded">
		<table class='table table-sm table-striped'>
			<tbody>
				<tr v-for='s in sites'>
					<td><router-link :to="'/missing_articles/'+id+'/'+s.site">{{s.site}}</router-link></td>
					<td class='num'>{{s.articles}}</td>
				</tr>
			</tbody>
		</table>
	</div>
	<div v-else><i tt='loading'></i></div>
</div>`
});
