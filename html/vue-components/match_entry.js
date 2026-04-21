import { entryDisplayMixin, editEntryMixin, entryMixin } from './mnm-mixins.js';
import { mnm_api, mnm_fetch_json, ensure_catalog, get_specific_catalog, tt_update_interface, tt, widar } from './store.js';

export default {
    name: 'match-entry',
    mixins: [entryDisplayMixin, editEntryMixin, entryMixin],
    props: ['entry'],
    data: function () {
        return {
            catalog: {}, mnm_entries: [], wp_entries: [], wd_entries: [],
            loaded_wp: false, loaded_mnm: false, loaded_wd: false,
            loaded_sparql: false, sparql_entries: [], last_id: ''
        };
    },
    created: function () { this.loadData(); },
    updated: function () { tt_update_interface(); },
    mounted: function () { tt_update_interface() },
    methods: {
        loadDataWikipedia: async function () {
            const me = this;
            me.loaded_wp = false;
            me.wp_entries = [];
            try {
                var d = await mnm_fetch_json('https://' + me.catalog.search_wp + '.wikipedia.org/w/api.php', {
                    action: 'query',
                    list: 'search',
                    format: 'json',
                    origin: '*',
                    srsearch: me.filteredName()
                });
                me.wp_entries = d.query.search;

                var titles = [];
                me.wp_entries.forEach(function (v) {
                    titles.push(v.title);
                });

                if (titles.length == 0) return;

                // Get matching Wikidata items
                var site = me.catalog.search_wp + 'wiki';
                try {
                    var d2 = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
                        action: 'wbgetentities',
                        props: 'sitelinks',
                        titles: titles.join('|'),
                        sites: site,
                        format: 'json',
                        origin: '*'
                    });
                    Object.entries(d2.entities).forEach(function ([q, v]) {
                        if (q * 1 < 0) return;
                        if (typeof v.sitelinks == 'undefined' || typeof v.sitelinks[site] == 'undefined') return;
                        var title = v.sitelinks[site].title;
                        var a = "<a target='_blank' class='wikidata' href='https://www.wikidata.org/wiki/" + q + "'>" + q + "</a>";
                        a += " [<a href='#' q='" + q + "' class='set_q' tt_title='manually_set_q'>&uarr;</a>]";
                        var td = document.querySelector('td.wd_loading[wp_title="' + me.normaliseTitle(title) + '"]');
                        if (td) { td.classList.remove('wd_loading'); td.innerHTML = a; }
                        var link = document.querySelector('a.set_q[q="' + q + '"]');
                        if (link) link.addEventListener('click', function (e) {
                            document.getElementById('q_input').value = q;
                            me.setUserQ(e);
                            e.preventDefault();
                        });
                    });
                } finally {
                    document.querySelectorAll('td.wd_loading').forEach(function (el) { el.classList.remove('wd_loading'); el.innerHTML = '&mdash;'; });
                    tt_update_interface();
                }
            } finally {
                me.loaded_wp = true;
            }
        },
        checkSPARQL: async function () {
            // TODO search would be faster?
            const me = this;
            me.loaded_sparql = false;
            me.sparql_entries = [];
            if (me.catalog.wd_prop == null) return;
            if (me.catalog.wd_qual != null) return;
            var sparql = 'SELECT ?q ?qLabel ?description { ?q wdt:P' + me.catalog.wd_prop + ' "' + me.entry.ext_id + '" ';
            sparql += 'OPTIONAL { ?q schema:description ?description filter(lang(?description)="en") } ';
            sparql += 'SERVICE wikibase:label { bd:serviceParam wikibase:language "en" } ';
            sparql += '}';
            var url = 'https://query.wikidata.org/sparql';
            var d = await mnm_fetch_json(url, { format: 'json', query: sparql });
            var qs = [];
            d.results.bindings.forEach(function (v) {
                if (v.q.type != 'uri') return;
                var vurl = v.q.value;
                var o = { id: vurl.replace(/^.+\/Q/, 'Q'), url: vurl };
                if ((v.qLabel || {}).type == 'literal') o.label = v.qLabel.value;
                if ((v.description || {}).type == 'literal') o.description = v.description.value;
                qs.push(o);
            });
            if (qs.length == 0) return;
            me.sparql_entries = qs;
            me.loaded_sparql = true;
        },
        normaliseTitle: function (t) {
            return encodeURIComponent(t.replace(/ /g, '_'));
        },
        loadData: async function () {
            const me = this;
            await ensure_catalog(me.entry.catalog);
            me.catalog = get_specific_catalog(me.entry.catalog) || {};
            if (!me.catalog.search_wp) me.catalog.search_wp = 'en';

            //			if ( me.entry.id == me.last_id ) return ;
            me.last_id = me.entry.id;

            me.loaded_mnm = false;
            me.mnm_entries = [];
            mnm_api('search', { what: me.filteredName(), max: 20 }).then(function (d) {
                Object.entries(d.data.entries).forEach(function ([k, v]) {
                    if (typeof d.data.users[v.user] == 'undefined') return;
                    d.data.entries[k].username = d.data.users[v.user].name;
                });
                Object.values(d.data.entries).forEach(function (v) {
                    if (v.id == me.entry.id) return;
                    me.mnm_entries.push(v);
                });
            }).catch(function () {}).then(function () { me.loaded_mnm = true });

            me.loaded_wd = false;
            me.wd_entries = [];
            mnm_fetch_json('https://www.wikidata.org/w/api.php', {
                action: 'wbsearchentities',
                search: me.filteredName(),
                language: me.catalog.search_wp,
                limit: 20,
                type: 'item',
                format: 'json',
                origin: '*'
            }).then(function (d) {
                (d.search || []).forEach(function (v) {
                    if (v.repository != 'local' && v.repository != 'wikidata') return;
                    me.wd_entries.push(v);
                });
            }).catch(function () {}).then(function () { me.loaded_wd = true });

            me.loadDataWikipedia();
            me.checkSPARQL();

        },
        wikipediaSearch: function () {
            var lang = this.catalog.search_wp;
            return "https://" + lang + ".wikipedia.org/w/index.php?title=Special%3ASearch&search=" + this.getSearchString(false);
        },
        wikidataSearch: function () {
            return "https://www.wikidata.org/w/index.php?button=&title=Special%3ASearch&search=" + encodeURIComponent(this.entry.ext_name || '');
        },
        // Offer the infernal initial-search when the entry is a Q5 whose
        // ext_name contains at least one initial — matches the same gating
        // used by the unmatched entry row in entry_list_item.js.
        hasInitials: function () {
            if (!this.entry || this.entry.type !== 'Q5') return false;
            let name = this.entry.ext_name || '';
            return /\b[A-Z]\./.test(name);
        },
        initialSearchUrl: function () {
            return 'https://wd-infernal.toolforge.org/initial_search/'
                + encodeURIComponent(this.entry.ext_name || '')
                + '?format=html';
        },
        qWasSet: function () {
            var btn = document.querySelector('button.load-random-entry');
            if (btn) btn.click(); // Load next entry
        },
        setUserQ: function (e) {
            e.preventDefault();
            var q = document.getElementById('q_input').value;
            if (!q.match(/\d/)) return false;
            this.setEntryQ(this.entry, q);
            this.qWasSet();
            return false;
        },
        setUserNoWD: function (e) {
            e.preventDefault();
            this.setEntryQ(this.entry, -1, true);
            this.qWasSet();
            return false;
        },
        setUserNA: function (e) {
            e.preventDefault();
            this.setEntryQ(this.entry, 0, true);
            this.qWasSet();
            return false;
        },
        setUserNew: function (e) {
            e.preventDefault();
            this.newItemForEntry(this.entry, function (q) {
                var url = "https://www.wikidata.org/wiki/Q" + q;
                var win = window.open(url, '_blank');
                //				win.focus() ;
            });
            this.qWasSet();
            return false;
        },

    },
    watch: {
        '$route'(to, from) {
            this.loadData();
        },
        'entry.q'(to, from) {
            if (to != null) return;
            this.loadData();
        }
    },
    template: `
	<div>

		<div v-if="entry.q==null">

			<div> <!-- Actions -->
				<div class="card" style="margin-bottom:1em" v-if='widar.is_logged_in'>
					<div class="card-body">
						<h4 class="card-title" tt='enter_q_number'></h4>
						<div class="card-text">
							<input type='text' id='q_input' @keyup.enter="setUserQ" />
							<button class='btn btn-outline-primary' @click.prevent='setUserQ' tt='set_q'></button>
							<button class='btn btn-outline-success' @click.prevent='setUserNew' tt='new_item'></button>
							<!--<button class='btn btn-outline-warning' @click.prevent='setUserNoWD' tt='no_wikidata_entry'></button>-->
							<button class='btn btn-outline-danger' @click.prevent='setUserNA' tt='n_a'></button>
						</div>
					</div>
				</div>
			</div>

			<div> <!-- Search links -->
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='search'></h4>
						<div class="card-text">
							<div class='btn-group btn-group-sm flex-wrap'>
								<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
									:href='wikidataSearch()' tt='search_wd'></a>
								<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
									:href='wikipediaSearch()' tt='search_wikipedia' :tt1='catalog.search_wp'></a>
								<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
									:href='"https://www.google.com/search?q="+getSearchString()+"+site%3Awikipedia.org"'
									tt='google_wikipedia'></a>
								<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
									:href='"https://www.google.com/search?q="+getSearchString()+"+site%3Awikisource.org"'
									tt='google_wikisource'></a>
								<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
									:href='"https://www.google.com/search?q="+getSearchString()+"+site%3Awikidata.org"'
									tt='google_wikidata'></a>
								<a v-if='hasInitials()' target='_blank' class='btn btn-outline-secondary mnm-action-btn'
									:href='initialSearchUrl()'
									title='Search Wikidata for people whose names expand to these initials'>Initial search</a>
							</div>
						</div>
					</div>
				</div>
			</div>

			<!-- SPARQL -->
			<div v-if="loaded_sparql">
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title">SPARQL results</h4>
						<div class="card-text">

							<div v-if="wd_entries.length>0" class="results_overflow_box">
								<table class="table table-sm table-striped">
									<tbody>
										<tr v-for="e in sparql_entries">
											<td nowrap>
												<a class='wikidata' target='_blank' :href='e.url'>{{e.id}}</a>
												[<a href='#'
													@click.prevent="\$event.preventDefault();setEntryQ(entry,e.id);qWasSet();return false"
													tt_title='manually_set_q'>&uarr;</a>]
											</td>
											<td style='width:100%'>
												<div><b>{{e.label}}</b></div>
												<div style='font-size:10pt;-family:serif;'>
													<wd-desc :item='e.id' autodesc_fallback='1'></wd-desc>
												</div>
											</td>
										</tr>
									</tbody>
								</table>
							</div>

						</div>
					</div>
				</div>
			</div>


			<div> <!-- Wikidata search results -->
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='wikidata_search_results'></h4>
						<div class="card-text">
							<div v-if="loaded_wd">
								<div v-if="wd_entries.length>0" class="results_overflow_box">
									<table class="table table-sm table-striped">
										<tbody>
											<tr v-for="e in wd_entries">
												<td nowrap>
													<a class='wikidata' target='_blank' :href='e.url'>{{e.id}}</a>
													[<a href='#'
														@click.prevent="\$event.preventDefault();setEntryQ(entry,e.id);qWasSet();return false"
														tt_title='manually_set_q'>&uarr;</a>]
												</td>
												<td style='width:100%'>
													<div><b>{{e.label}}</b></div>
													<div style='font-size:10pt;-family:serif;'>
														<wd-desc :item='e.id.replace(/\\D/g,"")'
															autodesc_fallback='1'></wd-desc>
													</div>
												</td>
											</tr>
										</tbody>
									</table>
								</div>
								<div v-else tt='no_matches'></div>
							</div>
							<div v-else tt='loading'></div>
						</div>
					</div>
				</div>
			</div>


			<div> <!-- Wikipedia search results -->
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title" tt='wikipedia_search_results' :tt1='catalog.search_wp'></h4>
						<div class="card-text">
							<div v-if="loaded_wp">
								<div v-if="wp_entries.length>0" class="results_overflow_box">
									<table class="table table-sm table-striped">
										<tbody>
											<tr v-for="e in wp_entries">
												<td><a target="_blank" class="external"
														:href="'https://'+catalog.search_wp+'.wikipedia.org/wiki/'+encodeURIComponent(e.title.replace(/ /g,'_')).replace(/'/g,'%27')">{{e.title|decodeEntities|removeTags|miscFixes}}</a>
												</td>
												<td class='wp_search_result_summary'>
													{{e.snippet|decodeEntities|removeTags|miscFixes}}</td>
												<td nowrap class="wd_loading" :wp_title="normaliseTitle(e.title)"><i
														tt='loading'></i></td>
											</tr>
										</tbody>
									</table>
								</div>
								<div v-else tt='no_matches'></div>
							</div>
							<div v-else tt='loading'></div>
						</div>
					</div>
				</div>
			</div>

			<div> <!-- Mix'n'match search results -->
				<div class="card" style="margin-bottom:1em">
					<div class="card-body">
						<h4 class="card-title">
							<span tt='results_other_catalogs'></span>
							<span style='font-size:9pt'>
								[<router-link :to='"/creation_candidates/by_ext_name/?ext_name="+entry.ext_name'
									tt='creation_candidates'></router-link>]
							</span>
						</h4>
						<div class="card-text"></div>
						<div v-if='loaded_mnm'>
							<div v-if="mnm_entries.length>0" class="results_overflow_box">
								<entry-list-item v-for="e in mnm_entries" :entry="e" :show_catalog="1"
									:show_permalink="1" :twoline="1" :key="e.id" setq='1'
									@onsetq='setEntryQ(entry,\$event)'></entry-list-item>
							</div>
							<div v-else tt='no_matches'></div>
						</div>
						<div v-else tt='loading'></div>
					</div>
				</div>
			</div>

		</div> <!-- v-if q==null -->

	</div>
`
};
