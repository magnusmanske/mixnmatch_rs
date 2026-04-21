import { mnm_api, mnm_fetch_json, mnm_notify, ensure_catalog, get_specific_catalog, widar, escapeHtml, wd } from './store.js';

(function() {
  const s = document.createElement('style');
  s.textContent = `/* ===== Mobile-match component styles ===== */
.mm-searchmatch { background:#fff176; border-radius:2px; padding:0 1px; }
.mm-datematch  { background:#f8bbd0; border-radius:2px; padding:0 1px; }

@keyframes mm-scorePop { 0%{transform:scale(1)} 50%{transform:scale(1.35)} 100%{transform:scale(1)} }
.mm-score-pop { animation: mm-scorePop 0.3s ease-out; }

.mm-entry-card {
    border-left:4px solid #a9c5eb; margin:8px; padding:10px 12px;
    background:#f8f9fa; border-radius:0 6px 6px 0;
}
.mm-entry-name { font-size:1.1rem; font-weight:600; word-break:break-word; }
.mm-entry-name a { color:inherit; text-decoration:underline; text-decoration-color:#a9c5eb; }
.mm-entry-meta { margin:4px 0; display:flex; flex-wrap:wrap; gap:6px; align-items:center; }
.mm-entry-dates { font-size:0.82rem; color:#6c757d; }
.mm-entry-desc { font-size:0.88rem; color:#555; margin-top:4px; line-height:1.4; }

.mm-skel-line {
    background:linear-gradient(90deg,#e9ecef 25%,#d5d9dd 50%,#e9ecef 75%);
    background-size:200% 100%; animation:mm-shimmer 1.2s infinite;
    border-radius:4px; height:14px; margin:6px 8px;
}
@keyframes mm-shimmer { 0%{background-position:200% 0} 100%{background-position:-200% 0} }

.mm-result-card {
    display:flex; align-items:stretch; border:1px solid #dee2e6;
    border-radius:8px; margin:6px 0; overflow:hidden; transition:background 0.15s;
}
.mm-result-card:hover { background:#fafafa; }
.mm-result-body { flex:1; padding:10px 12px; min-width:0; }
.mm-result-title { font-weight:600; font-size:1rem; word-break:break-word; }
.mm-result-title a { color:#36c; text-decoration:none; }
.mm-result-title a:hover { text-decoration:underline; }
.mm-result-desc { font-size:0.82rem; color:#666; margin-top:3px; line-height:1.4; }
.mm-result-desc .mm-pending { color:#767676; font-style:italic; }
.mm-result-action {
    display:flex; flex-direction:column; align-items:center; justify-content:center;
    padding:8px; background:#f8f9fa; border-left:1px solid #dee2e6; flex-shrink:0; gap:2px;
}
.mm-btn-set { min-width:56px; min-height:52px; font-size:0.88rem; line-height:1.2; }
.mm-shortcut-hint { font-size:10px; color:#767676; }

@keyframes mm-matchFlash { 0%{background-color:#d4edda} 100%{background-color:transparent} }
.mm-result-card.mm-match-flash { animation: mm-matchFlash 0.5s ease-out; }

.mm-footer-bar {
    position:fixed; bottom:0; left:0; right:0; background:#fff;
    border-top:1px solid #dee2e6; padding:8px 10px; z-index:100;
}
.mm-footer-btn {
    flex:1; min-height:52px; display:flex; flex-direction:column;
    align-items:center; justify-content:center; line-height:1.25; font-size:0.8rem;
}
.mm-footer-btn .mm-fb-label { font-weight:700; font-size:0.92rem; }
.mm-no-results { text-align:center; padding:28px 16px; color:#666; }
.mm-no-results .mm-nr-icon { font-size:2.5rem; margin-bottom:8px; }
.mm-main-content { padding-bottom:80px; }`;
  document.head.appendChild(s);
})();

export default Vue.extend({
  props: ['id'],
  data: function () {
    return {
      catalog_id: null,
      catalog_name: '',
      current_entry: {},
      entry_visible: false,
      loading_candidates: false,
      no_results: false,
      candidates: {},
      sorted_results: [],
      buttons_disabled: false,
      score: 0,
      remaining: 0,
      last_created_q: null,
      widar_ready: false,
      wd_local: null,
      wp_query: '',
      running: 0,
      max_order: 15,
      bad_titles: [/^List of /, /^History of /, /^Geography of /, /^High Sheriff /, /^Liste /],
      _keyHandler: null,
      _touchStartX: 0,
      _touchStartY: 0,
      _touchStartHandler: null,
      _touchEndHandler: null
    }
  },
  computed: {
    entry_type: function () {
      var e = this.current_entry;
      if (e.type) return e.type;
      var cat = get_specific_catalog(this.catalog_id);
      return cat ? cat.type : '';
    },
    entry_dates: function () {
      var e = this.current_entry;
      var born = (e.born || '').replace(/-00/g, '');
      var died = (e.died || '').replace(/-00/g, '');
      if (!born && !died) return '';
      return (born || '?') + (died ? ' \u2013 ' + died : '');
    },
  },
  created: function () {
    const me = this;
    me.wd_local = new WikiData();
    me.score = parseInt(sessionStorage.getItem('mg_matched') || '0');
    if (me.id) {
      me.catalog_id = me.id;
    }
    me.checkWidar();
  },
  mounted: function () {
    const me = this;
    me.setupKeyboard();
  },
  beforeDestroy: function () {
    this.teardownKeyboard();
  },
  watch: {
    '$route': function (to) {
      const me = this;
      if (to.params.id && to.params.id != me.catalog_id) {
        me.catalog_id = to.params.id;
        me.loadRandom();
      }
    }
  },
  methods: {
    checkWidar: function () {
      const me = this;
      // Wait for the global widar component to be ready
      function poll() {
        if (typeof widar !== 'undefined' && widar.loaded) {
          me.widar_ready = widar.is_logged_in;
          if (me.widar_ready && me.catalog_id) me.loadRandom();
        } else {
          setTimeout(poll, 100);
        }
      }
      poll();
    },
    selectCatalog: function (id) {
      this.$router.push('/mobile_match/' + id);
    },
    wdLink: function (q) {
      return 'https://' + (screen.width > screen.height ? 'www' : 'm') + '.wikidata.org/wiki/' + q;
    },

    // === Highlighting ===
    escapeHtml: function (str) {
      return escapeHtml(str || '');
    },
    findFirstFourDigitNumber: function (str) {
      var m = (str || '').match(/\b[1-9]\d{3}\b/);
      return m ? parseInt(m[0]) : null;
    },
    highlight: function (h) {
      if (!h) return '';
      const me = this;
      var words = (me.wp_query || '').split(/\s+/);
      var born = me.findFirstFourDigitNumber(me.current_entry.born || '');
      var died = me.findFirstFourDigitNumber(me.current_entry.died || '');
      if (born) words.push('' + born);
      if (died) words.push('' + died);
      words.forEach(function (word) {
        word = ('' + word).trim();
        if (!word || word === 'null' || word === 'undefined') return;
        var cls = isNaN(word) ? 'mm-searchmatch' : 'mm-datematch';
        var escaped = word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        var r = new RegExp('\\b(' + escaped + ')\\b', 'ig');
        h = h.replace(r, "<span class='" + cls + "'>$1</span>");
      });
      return h;
    },

    // === WiDaR wrapper ===
    getWidar: async function (p, callback, _retries) {
      const me = this;
      const maxRetries = 5;
      _retries = _retries || 0;
      p.query = 'widar';
      p.tool_hashtag = "mix'n'match-mobile-game";
      try {
        var resp = await fetch('./api.php?' + new URLSearchParams(p));
        var d = await resp.json();
        if (d.error != 'OK') {
          var retryable = /Invalid token|happen|Problem creating item/.test(d.error) ||
            (p.action != 'create_redirect' && /failed/.test(d.error));
          if (_retries < maxRetries && retryable) {
            setTimeout(function () { me.getWidar(p, callback, _retries + 1); }, 500 * (_retries + 1));
          } else {
            if (_retries >= maxRetries) mnm_notify('Wikidata edit failed after ' + maxRetries + ' retries', 'danger');
            callback(d);
          }
        } else {
          callback(d);
        }
      } catch (e) {
        if (_retries < maxRetries) {
          setTimeout(function () { me.getWidar(p, callback, _retries + 1); }, 1000 * (_retries + 1));
        } else {
          mnm_notify('Network error: could not reach API after ' + maxRetries + ' retries', 'danger');
          callback({ error: 'Network error after ' + maxRetries + ' retries' });
        }
      }
    },

    // === Match API calls ===
    matchEntryQ: async function (q, callback) {
      const me = this;
      q = ('' + q).replace(/\D/g, '');
      try {
        var d = await mnm_api('match_q', {
          tusc_user: widar.getUserName(), entry: me.current_entry.id, q: q
        }, { method: 'POST' });
        if (q * 1 > 0 && d.entry && d.entry.wd_prop && parseInt(d.entry.wd_prop) > 0 &&
          d.entry.wd_qual === null && !(d.entry.ext_id || '').match(/^fake_id_/)) {
          var prop = 'P' + ('' + d.entry.wd_prop).replace(/\D/g, '');
          var qid = 'Q' + q;
          me.getWidar({ botmode: 1, action: 'set_string', id: qid, prop: prop, text: d.entry.ext_id }, function () { callback(); });
        } else {
          callback();
        }
      } catch (e) {
        mnm_notify(e.message || 'Match failed', 'danger');
        throw e;
      }
    },
    matchEntrySpecial: async function (qValue, callback) {
      try {
        await mnm_api('match_q', {
          tusc_user: widar.getUserName(), entry: this.current_entry.id, q: qValue
        }, { method: 'POST' });
        callback();
      } catch (e) {
        mnm_notify(e.message || 'Match failed', 'danger');
        throw e;
      }
    },
    matchEntryToNewQ: async function (q, callback) {
      try {
        await mnm_api('match_q', {
          tusc_user: widar.getUserName(), entry: this.current_entry.id, q: q
        }, { method: 'POST' });
        callback();
      } catch (e) {
        mnm_notify(e.message || 'Match failed', 'danger');
        throw e;
      }
    },

    // === Actions ===
    incrementScore: function () {
      this.score++;
      sessionStorage.setItem('mg_matched', this.score);
      const me = this;
      Vue.nextTick(function () {
        var el = me.$refs.scoreBadge;
        if (el) { el.classList.add('mm-score-pop'); setTimeout(function () { el.classList.remove('mm-score-pop'); }, 400); }
      });
    },
    setMatch: function (q, idx) {
      const me = this;
      me.buttons_disabled = true;
      Vue.set(me.sorted_results[idx], 'flashing', true);
      try {
        me.matchEntryQ(q, function () {
          me.incrementScore();
          setTimeout(function () { me.buttons_disabled = false; me.loadRandom(); }, 400);
        }).catch(function () {
          me.buttons_disabled = false;
        });
      } catch (e) {
        me.buttons_disabled = false;
      }
    },
    createItem: async function () {
      const me = this;
      if (!me.current_entry.id) return;
      me.buttons_disabled = true;
      await ensure_catalog(me.catalog_id);
      var catalog = get_specific_catalog(me.catalog_id);
      try {
        var d = await mnm_api('prep_new_item', { entry_ids: me.current_entry.id, default_entry: 0 });
        if (typeof d.data == 'undefined') {
          mnm_notify('Problem preparing new item: unknown error', 'danger');
          me.buttons_disabled = false;
          return;
        }
        var summary = 'New item based on [[:toollabs:mix-n-match/#/entry/' +
          me.current_entry.id + '|' + me.current_entry.ext_name + ' (#' + me.current_entry.id + ')]]' +
          (catalog ? ' in [[:toollabs:mix-n-match/#/catalog/' + catalog.id + '|' + catalog.name + ']]' : '');
        me.getWidar({
          botmode: 1, action: 'generic', summary: summary,
          json: JSON.stringify({ action: 'wbeditentity', 'new': 'item', data: d.data })
        }, function (wd_result) {
          if (!wd_result.res || !wd_result.res.entity || !wd_result.res.entity.id) {
            mnm_notify('Problem creating item on Wikidata', 'danger');
            me.buttons_disabled = false;
            return;
          }
          var q = wd_result.res.entity.id.replace(/\D/g, '');
          if (!q || q === '0') { me.buttons_disabled = false; return; }
          window.open('https://www.wikidata.org/wiki/Q' + q, '_blank');
          me.last_created_q = q;
          me.matchEntryToNewQ(q, function () {
            me.incrementScore();
            me.buttons_disabled = false;
            me.loadRandom();
          });
        });
      } catch (e) {
        mnm_notify('Problem preparing new item: ' + e.message, 'danger');
        me.buttons_disabled = false;
      }
    },
    markNA: function () {
      const me = this;
      if (!me.current_entry.id) return;
      me.buttons_disabled = true;
      try {
        me.matchEntrySpecial(0, function () {
          me.incrementScore();
          me.buttons_disabled = false;
          me.loadRandom();
        }).catch(function () {
          me.buttons_disabled = false;
        });
      } catch (e) {
        me.buttons_disabled = false;
      }
    },
    skipEntry: function () {
      this.loadRandom();
    },

    // === Search ===
    badQ: function (q) {
      var wd = this.wd_local;
      if (!wd.items[q]) return true;
      if (wd.items[q].hasClaimItemLink('P31', 'Q4167410')) return true;
      if (wd.items[q].hasClaimItemLink('P31', 'Q13406463')) return true;
      if (wd.items[q].hasClaimItemLink('P31', 'Q4167836')) return true;
      return false;
    },
    searchDone: async function () {
      const me = this;
      me.running--;
      if (me.running > 0) return;
      me.loading_candidates = false;

      var rk = [];
      Object.entries(me.candidates).forEach(function ([k, v]) {
        if (me.current_entry.type == 'Q5' && !me.wd_local.getItem(k).hasClaimItemLink('P31', 'Q5')) return;
        rk.push(k);
        v.points = 0;
        v.points += me.max_order - v.wd_order;
        v.points += me.max_order - v.wp_order;
        if (me.current_entry.ext_name == (v.title || '').replace(/ \(.+$/, '') ||
          me.current_entry.ext_name == (v.label || '')) {
          v.points += me.max_order + 5;
        }
      });

      if (rk.length == 0) {
        me.no_results = true;
        setTimeout(function () { me.no_results = false; me.loadRandom(); }, 2000);
        return;
      }

      rk.sort(function (a, b) { return me.candidates[b].points - me.candidates[a].points; });

      var sorted = [];
      await ensure_catalog(me.catalog_id);
      var _cat1 = get_specific_catalog(me.catalog_id);
      var lang = _cat1 ? _cat1.search_wp : 'en';
      rk.forEach(function (q) {
        var e = me.candidates[q];
        sorted.push({ q: q, title: e.title, label: e.label, snippet: e.snippet, points: e.points, autodesc: null, flashing: false });
        // Load autodesc async
        mnm_fetch_json('https://autodesc.toolforge.org/', {
          lang: lang, mode: 'short', links: 'text', redlinks: '', format: 'json', q: q
        }).then(function (d) {
          for (var i = 0; i < me.sorted_results.length; i++) {
            if (me.sorted_results[i].q === q) {
              Vue.set(me.sorted_results[i], 'autodesc', d.result ? me.highlight(d.result) : '<span class="mm-pending">\u2014</span>');
              break;
            }
          }
        });
      });
      me.sorted_results = sorted;
    },
    doWikipediaSearch: async function () {
      const me = this;
      try {
        me.wp_query = (me.current_entry.ext_name || '').replace(/_/g, ' ').replace(/\s\(.+/, '');
        if (me.current_entry.type == 'Q5') {
          var m = (me.current_entry.ext_desc || '').match(/\b\d{3,4}\b/g);
          if (m !== null) { while (m.length > 2) m.pop(); me.wp_query += ' ' + m.join(' '); }
        }
        await ensure_catalog(me.catalog_id);
        var _cat2 = get_specific_catalog(me.catalog_id);
        var lang = _cat2 ? _cat2.search_wp : 'en';
        var d = await mnm_fetch_json('https://' + lang + '.wikipedia.org/w/api.php', {
          action: 'query', list: 'search', srsearch: me.wp_query, srnamespace: 0,
          srwhat: 'text', srprop: 'snippet', format: 'json', origin: '*'
        });
        var titles = [], tmp = {};
        ((d.query || {}).search || []).forEach(function (v, k) {
          var bad = false;
          var t = v.title.replace(/_/g, ' ');
          me.bad_titles.forEach(function (v0) { if (v0.test(t)) bad = true; });
          if (bad) return;
          titles.push(v.title); v.order = k;
          tmp[v.title.replace(/_/g, ' ')] = v;
        });
        if (titles.length == 0) return me.searchDone();
        var d2 = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
          action: 'wbgetentities', sites: lang + 'wiki', titles: titles.join('|'), format: 'json', languages: lang, origin: '*'
        });
        Object.entries(d2.entities).forEach(function ([q, v]) {
          if (!v.sitelinks || !v.sitelinks[lang + 'wiki']) return;
          var title = v.sitelinks[lang + 'wiki'].title.replace(/_/g, ' ');
          if (!tmp[title]) return;
          if (!me.wd_local.items[q]) me.wd_local.items[q] = new WikiDataItem(me.wd_local, v);
          if (me.badQ(q)) return;
          if (!me.candidates[q]) me.candidates[q] = { q: q, cnt: 0, wd_order: me.max_order };
          me.candidates[q].cnt++;
          me.candidates[q].title = title;
          me.candidates[q].snippet = tmp[title].snippet;
          me.candidates[q].wp_order = tmp[title].order;
        });
      } catch (e) {
        console.error('Wikipedia search failed:', e.message);
      }
      me.searchDone();
    },
    doWikidataSearch: async function () {
      const me = this;
      try {
        await ensure_catalog(me.catalog_id);
        var _cat3 = get_specific_catalog(me.catalog_id);
        var lang = _cat3 ? _cat3.search_wp : 'en';
        var d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
          action: 'wbsearchentities', search: me.current_entry.ext_name, format: 'json',
          language: lang, uselang: lang, type: 'item', limit: 10, continue: 0, origin: '*'
        });
        var qs = [];
        (d.search || []).forEach(function (v) { qs.push(v.id); });
        me.wd_local.getItemBatch(qs).then(function () {
          qs.forEach(function (q, rank) {
            if (me.badQ(q)) return;
            if (!me.candidates[q]) me.candidates[q] = { q: q, cnt: 0, wp_order: me.max_order };
            me.candidates[q].cnt++;
            me.candidates[q].label = me.wd_local.items[q].getLabel();
            me.candidates[q].wd_order = rank;
          });
          me.searchDone();
        });
      } catch (e) {
        console.error('Wikidata search failed:', e.message);
        me.searchDone();
      }
    },

    // === Load random entry ===
    loadRandom: async function () {
      const me = this;
      me.entry_visible = false;
      me.loading_candidates = true;
      me.no_results = false;
      me.sorted_results = [];
      me.candidates = {};

      await ensure_catalog(me.catalog_id);
      var cat = get_specific_catalog(me.catalog_id);
      if (cat) { me.catalog_name = cat.name; me.remaining = cat.noq * 1; }

      try {
        var d = await mnm_api('random', { submode: 'unmatched', catalog: me.catalog_id });
        if (!d.data) {
          me.loading_candidates = false;
          me.current_entry = {};
          me.entry_visible = true;
          mnm_notify('No more unmatched entries \u2014 this catalog may be fully matched! \ud83c\udf89', 'success', 8000);
          return;
        }
        me.current_entry = d.data;
        me.entry_visible = true;
        me.running = 2;
        me.doWikipediaSearch();
        me.doWikidataSearch();
      } catch (e) {
        me.loading_candidates = false;
        me.current_entry = {};
        me.entry_visible = true;
        mnm_notify('Failed to load entry: ' + e.message, 'danger', 8000);
      }
    },

    // === Keyboard / touch ===
    setupKeyboard: function () {
      const me = this;
      me._keyHandler = function (e) {
        if (e.target.matches('input, textarea, select')) return;
        if (!me.catalog_id) return;
        if (e.key === 'ArrowRight' || e.key === ' ') { e.preventDefault(); me.skipEntry(); }
        else if (e.key === 'ArrowLeft') { e.preventDefault(); me.createItem(); }
        else if (e.key === 'Enter' && me.sorted_results.length) { e.preventDefault(); me.setMatch(me.sorted_results[0].q, 0); }
        else if (e.key >= '1' && e.key <= '9') {
          var idx = parseInt(e.key) - 1;
          if (idx < me.sorted_results.length) { e.preventDefault(); me.setMatch(me.sorted_results[idx].q, idx); }
        }
      };
      document.addEventListener('keydown', me._keyHandler);

      me._touchStartHandler = function (e) { me._touchStartX = e.touches[0].clientX; me._touchStartY = e.touches[0].clientY; };
      me._touchEndHandler = function (e) {
        var dx = e.changedTouches[0].clientX - me._touchStartX;
        var dy = e.changedTouches[0].clientY - me._touchStartY;
        if (Math.abs(dx) > 90 && Math.abs(dx) > Math.abs(dy) * 2.5 && dx < 0) me.skipEntry();
      };
      document.addEventListener('touchstart', me._touchStartHandler, { passive: true });
      document.addEventListener('touchend', me._touchEndHandler, { passive: true });
    },
    teardownKeyboard: function () {
      if (this._keyHandler) document.removeEventListener('keydown', this._keyHandler);
      if (this._touchStartHandler) document.removeEventListener('touchstart', this._touchStartHandler);
      if (this._touchEndHandler) document.removeEventListener('touchend', this._touchEndHandler);
    }
  },
  template: `<div class='mt-2'>
    <mnm-breadcrumb :crumbs="[{text: 'Quick match'}]"></mnm-breadcrumb>
    <div v-if='!widar_ready'>
        <div class='alert alert-warning m-3'>
            <a href='/widar/index.php?action=authorize' target='_blank'>Log into WiDaR</a>
            to make edits, then reload this page.
        </div>
    </div>
    <div v-else>
        <!-- Score bar -->
        <div v-if='catalog_id' class='d-flex gap-2 flex-wrap px-2 mb-1'>
            <span class='badge bg-secondary' v-if='catalog_name'>{{catalog_name}}</span>
            <span ref='scoreBadge' class='badge bg-success'>{{score}} matched</span>
            <span v-if='remaining>0' class='badge bg-secondary'>{{remaining}} remaining</span>
            <a v-if='last_created_q' class='badge bg-info text-decoration-none' target='_blank'
                :href="'https://www.wikidata.org/wiki/Q'+last_created_q">Q{{last_created_q}} &#x2197;</a>
        </div>

        <!-- No catalog selected: redirect to main page -->
        <div v-if='!catalog_id' class='p-3'>
            <div class='alert alert-info'>Please select a catalog from the <router-link to='/'>main page</router-link> first, then use Quick match from the catalog&rsquo;s Action menu.</div>
        </div>

        <!-- Entry + candidates -->
        <div v-if='catalog_id' class='mm-main-content'>
            <!-- Entry card -->
            <div v-if='entry_visible' class='mm-entry-card'>
                <div class='mm-entry-name'>
                    <a v-if='current_entry.ext_url' :href='current_entry.ext_url' target='_blank'>{{current_entry.ext_name}}</a>
                    <span v-else>{{current_entry.ext_name}}</span>
                    <router-link class='ms-2 text-decoration-none' :to="'/creation_candidates/by_ext_name/?ext_name='+encodeURIComponent(current_entry.ext_name||'')">&#x1F517;</router-link>
                </div>
                <div class='mm-entry-meta'>
                    <span v-if='entry_type' class='badge bg-secondary'>{{entry_type}}</span>
                    <span v-if='entry_dates' class='mm-entry-dates'>{{entry_dates}}</span>
                </div>
                <div v-if='current_entry.ext_desc' class='mm-entry-desc'>{{current_entry.ext_desc}}</div>
            </div>

            <!-- Skeleton loader -->
            <div v-if='loading_candidates'>
                <div class='mm-skel-line' style='width:55%;height:18px;margin-top:12px'></div>
                <div class='mm-skel-line' style='width:88%'></div>
                <div class='mm-skel-line' style='width:72%'></div>
                <div class='mm-skel-line mt-3' style='width:60%;height:18px'></div>
                <div class='mm-skel-line' style='width:90%'></div>
                <div class='mm-skel-line' style='width:78%'></div>
            </div>

            <!-- No results -->
            <div v-if='no_results' class='mm-no-results'>
                <div class='mm-nr-icon'>&#x1F50D;</div>
                <p class='mb-0'>No candidates found for this entry.<br><small class='text-muted'>Loading next entry...</small></p>
            </div>

            <!-- Candidate results -->
            <div v-for='(r,idx) in sorted_results' :key='r.q' :class="'mm-result-card'+(r.flashing?' mm-match-flash':'')">
                <div class='mm-result-body'>
                    <div class='mm-result-title'>
                        <a :href='wdLink(r.q)' target='_blank' v-html='highlight(r.title||r.label||r.q)'></a>
                    </div>
                    <div v-if='r.snippet' class='mm-result-desc' v-html='highlight(r.snippet)'></div>
                    <div class='mm-result-desc mm-autodesc' v-html='r.autodesc||"<span class=\\"mm-pending\\">...</span>"'></div>
                </div>
                <div class='mm-result-action'>
                    <div v-if='idx<9' class='mm-shortcut-hint'>{{idx+1}}</div>
                    <button class='btn btn-outline-success mm-btn-set' @click='setMatch(r.q,idx)' :disabled='buttons_disabled'
                        :title='idx===0?"Shortcut: 1 or Enter":(idx<9?"Shortcut: "+(idx+1):"")'>&#x2713; Set</button>
                </div>
            </div>

            <!-- Footer action bar -->
            <div class='mm-footer-bar d-flex gap-2'>
                <button class='btn btn-outline-success mm-footer-btn' @click='createItem' :disabled='buttons_disabled' title='Shortcut: left arrow'>
                    <span class='mm-fb-label'>Create</span><small>New WD item</small>
                </button>
                <button class='btn btn-outline-secondary mm-footer-btn' @click='markNA' :disabled='buttons_disabled'>
                    <span class='mm-fb-label'>N/A</span><small>Not applicable</small>
                </button>
                <button class='btn btn-outline-primary mm-footer-btn' @click='skipEntry' :disabled='buttons_disabled' title='Shortcut: right arrow or Space'>
                    <span class='mm-fb-label'>Skip &#x2192;</span><small>Next entry</small>
                </button>
            </div>
        </div>
    </div>
</div>`
});
