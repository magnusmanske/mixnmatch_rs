// https://tools.wmflabs.org/mix-n-match/#/search/Vase%20Painter/?include=1653,1,2,3
import { mnm_api, tt_update_interface, ensure_catalogs } from './store.js';

export const SearchBox = {
  name: 'search-box',
  props: ['query', 'exclude', 'include'],
  updated: function () { tt_update_interface() },
  mounted: function () { tt_update_interface() },
  methods: {
    submit: function () {
      const me = this;
      // The navbar mounts <search-box></search-box> with no props, so
      // me.query stays undefined until the user types. Guard against
      // submit-on-empty: encodeURIComponent(undefined) returns the
      // literal string "undefined", which used to produce
      // /search/undefined and render the word "undefined" in the
      // search input on landing.
      var q = (me.query || '').trim();
      if (!q) {
        this.$router.push('/search');
        return;
      }
      var url = '/search/' + encodeURIComponent(q);
      if (typeof me.exclude != 'undefined' && me.exclude.length > 0) {
        url += '/' + this.exclude.join(',');
      }
      if (typeof me.include != 'undefined' && me.include.length > 0) {
        url += '?include=' + me.include.join(',');
      }
      this.$router.push(url);
    }
  },
  template: `<form class="d-flex my-2 my-lg-0 search_box_form" @submit="submit();return false">
  <input class="form-control me-sm-2" type="text" tt_placeholder="search" v-model="query" accesskey="f">
  <button class="btn btn-outline-secondary my-2 my-sm-0" type="submit" tt="search"></button>
</form>`
};

// Allowed values for the match_status filter — must mirror the
// `SearchMatchStatus` enum exposed by the Rust API. Keep in sync.
const MATCH_STATUS_VALUES = ['any', 'matched', 'unmatched'];

export default Vue.extend({
  props: ['query', 'excl'],
  data: function () { return { results: [], exclude: [], include: [], exclude_catalogs: [], include_catalogs: [], running: true, show_exclude: false, show_include: false, match_status: 'any' } },
  created: function () {
    this.readMatchStatusFromRoute();
    this.updateResults();
  },
  updated: function () { tt_update_interface() },
  mounted: function () {
    tt_update_interface();
    var input = this.$el.querySelector('form.search_box_form input[type="text"]');
    if (input) input.focus();
  },
  methods: {
    readMatchStatusFromRoute: function () {
      var raw = this.$route.query.match_status;
      // Unknown / missing values fall back to "any" — matches the
      // backend's `SearchMatchStatus::from_param` so the UI and API
      // agree on degraded values from stale bookmarks.
      this.match_status = MATCH_STATUS_VALUES.indexOf(raw) >= 0 ? raw : 'any';
    },
    updateResults: async function () {
      const me = this;
      me.running = true;

      if (typeof me.excl != 'undefined') {
        me.exclude = me.excl.split(',');
        me.excl = undefined;
      } else me.exclude = [];

      if (typeof me.$route.query.include != 'undefined') {
        me.include = me.$route.query.include.replace(/[^0-9,]/g, '').split(/,/);
      }

      me.query = (me.query || '').trim();
      // Defensive: stale bookmarks of the old /search/undefined URL
      // would otherwise run a literal search for the word "undefined".
      if (me.query === 'undefined') me.query = '';
      if (me.query.match(/^\s*$/)) {
        me.running = false;
        return;
      }

      var input = document.querySelector('form.search_box_form input[type="text"]');
      if (input) input.value = me.query;
      try {
        var d = await mnm_api('search', { what: me.query, exclude: me.exclude.join(','), include: me.include.join(','), match_status: me.match_status }, { method: 'POST' });
        Object.values(d.data.entries).forEach(function (v) {
          if (d.data.users[v.user]) v.username = d.data.users[v.user].name;
        });
        var catalog_ids = [...new Set(Object.values(d.data.entries).map(function (e) { return e.catalog; }))];
        await ensure_catalogs(catalog_ids);
        me.results = d.data.entries;
      } catch (e) {
        console.error('Search failed', e);
      }
      me.running = false;
    },
    onMatchStatusChange: function () {
      // Persist the choice in the URL so back/forward and bookmarks
      // restore the same filter, then let the $route watcher trigger
      // the re-search rather than calling updateResults twice.
      var q = Object.assign({}, this.$route.query);
      if (this.match_status === 'any') delete q.match_status;
      else q.match_status = this.match_status;
      this.$router.push({ path: this.$route.path, query: q });
    },
    onExcludeChange: function (list) {
      this.exclude_catalogs = list;
      this.exclude = list.map(function (c) { return c.id; });
    },
    onIncludeChange: function (list) {
      this.include_catalogs = list;
      this.include = list.map(function (c) { return c.id; });
    }
  },
  watch: {
    '$route'(to, from) {
      this.readMatchStatusFromRoute();
      this.updateResults(to.params.query);
    }
  },
  template: `<div style='margin-top:10px'>
<mnm-breadcrumb :crumbs="[{text: 'Search'}]"></mnm-breadcrumb>

<div style='display:inline-block'><search-box :query="query" :exclude="exclude" :include="include"></search-box></div>
<div style='display:inline-block'><button class='btn btn-outline-secondary' @click.prevent='show_include=!show_include' tt='include_catalogs'></button></div>
<div style='display:inline-block'><button class='btn btn-outline-secondary' @click.prevent='show_exclude=!show_exclude' tt='exclude_catalogs'></button></div>
<div style='display:inline-block; margin-left:0.5em'><router-link to='/by_property_value' class='btn btn-outline-secondary'>Search by property value</router-link></div>

<div class='search-match-status'>
	<span class='search-match-status-label'>Match:</span>
	<label><input type='radio' v-model='match_status' value='any' @change='onMatchStatusChange' /><span tt='any'></span></label>
	<label><input type='radio' v-model='match_status' value='matched' @change='onMatchStatusChange' />Matched</label>
	<label><input type='radio' v-model='match_status' value='unmatched' @change='onMatchStatusChange' /><span tt='unmatched'></span></label>
</div>

<div v-if='show_exclude' class="card my-3">
	<div class="card-body">
	<p tt='exclude_box'></p>
	<catalog-search-picker :multi="true" :value="exclude_catalogs" @change="onExcludeChange" placeholder="Search catalogs to exclude..."></catalog-search-picker>
	</div>
</div>

<div v-if='show_include' class="card my-3">
	<div class="card-body">
	<p tt='include_box'></p>
	<catalog-search-picker :multi="true" :value="include_catalogs" @change="onIncludeChange" placeholder="Search catalogs to include..."></catalog-search-picker>
	</div>
</div>

<div v-if='(typeof query!="undefined")'>
<hr/>
<div v-if='running'><i tt='searching'></i></div>
<div v-else>
<div v-if='Object.keys(results).length>0'><entry-list-item v-for="e in results" :entry="e" :show_catalog="1" :show_permalink="1" :twoline="1" key="e.id"></entry-list-item></div>
<div v-else-if='!query.match(/^\\s*$/)'><i tt="no_matches"></i></div>
</div>
</div>
</div>`
});
