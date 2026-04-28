import { widar } from './store.js';

export default {
	name: 'catalog-actions-dropdown',
	props: ['catalog'],
	//	created : function () { tt.updateInterface(this.$el) } ,
	//	updated : function () { tt.updateInterface(this.$el) } ,
	//	mounted : function () { tt.updateInterface(this.$el) } ,
	methods: {
		getSearchCatalogPath: function () {
			const me = this;
			return "/search/?include=" + me.catalog.id;
		}
	},
	template: `
	<div class="dropdown-menu dropdown-menu-end" style="padding:2px" v-if="catalog">
		<router-link :to='"/list/"+catalog.id+"/manual"' class="dropdown-item" style="padding:2px"
			tt="manually_matched"></router-link>
		<router-link :to='"/list/"+catalog.id+"/auto"' class="dropdown-item" style="padding:2px"
			tt="auto_matched"></router-link>
		<router-link :to='"/list/"+catalog.id+"/unmatched"' class="dropdown-item" style="padding:2px"
			tt="unmatched"></router-link>
		<router-link :to='"/list/"+catalog.id+"/multi_match"' class="dropdown-item" style="padding:2px"
			tt="multi_match"></router-link>
		<!-- <router-link :to='"/list/"+catalog.id+"/nowd"' class="dropdown-item" style="padding:2px" tt="no_wikidata"></router-link> -->
		<router-link :to='"/list/"+catalog.id+"/na"' class="dropdown-item" style="padding:2px"
			tt="not_applicable"></router-link>
		<div class="dropdown-divider"></div>
		<router-link :to='"/import/"+catalog.id' class="dropdown-item" style="padding:2px"
			tt="import_or_update_catalog"></router-link>
		<router-link :to='"/site_stats/"+catalog.id' class="dropdown-item" style="padding:2px"
			tt="site_stats"></router-link>
		<router-link v-if='catalog.has_locations=="yes"' :to='"/map/"+catalog.id' class="dropdown-item"
			style="padding:2px;font-weight:bold" tt="map"></router-link>
		<router-link v-if='catalog.has_locations=="yes" || typeof catalog.image_pattern!="undefined"'
			:to='"/quick_compare/"+catalog.id' class="dropdown-item" style="padding:2px;font-weight:bold"
			tt="quick_compare"></router-link>
		<router-link :to='"/download/"+catalog.id' class="dropdown-item" style="padding:2px;font-weight:bold"
			tt="download"></router-link>
		<router-link :to='"/random/"+catalog.id' class="dropdown-item" style="padding:2px;font-weight:bold"
			tt="game_mode"></router-link>
		<router-link :to='"/rc/"+catalog.id' class="dropdown-item" style="padding:2px"
			tt="rc_for_catalog"></router-link>
		<router-link :to='"/aliases/"+catalog.id' class="dropdown-item" style="padding:2px" tt="aliases"></router-link>
		<router-link v-if='widar.is_logged_in' :to='"/jobs/"+catalog.id' class="dropdown-item" style="padding:2px"
			tt="jobs"></router-link>
		<router-link :to='getSearchCatalogPath()' class="dropdown-item" style="padding:2px"
			tt="search_this_catalog"></router-link>
		<router-link :to='"/common_names/"+catalog.id' class="dropdown-item" style="padding:2px;font-weight:bold;"
			tt="common_names"></router-link>
		<router-link v-if='catalog.statement_text_count > 0' :to='"/statement_text/"+catalog.id' class="dropdown-item"
			style="padding:2px" tt="statement_text_match"></router-link>
		<router-link :to='"/issues/ALL/"+catalog.id' class="dropdown-item" style="padding:2px"
			tt='issues_in_this_catalog'></router-link>
		<router-link v-if='catalog.wd_prop!=null && catalog.wd_qual==null' :to='"/sync/"+catalog.id'
			class="dropdown-item" style="padding:2px" tt="sync_catalog"></router-link>
		<router-link v-if='widar.is_catalog_admin || (widar.mnm_user_id && catalog.owner && widar.mnm_user_id==catalog.owner)'
			:to='"/catalog_editor/"+catalog.id' class="dropdown-item"
			style="padding:2px" tt="catalog_editor"></router-link>
		<!--
			Deep-link into the scraper wizard pre-loaded with this
			catalog's existing settings. Only shown when catalog_overview
			attached an autoscrape_json to the catalog object — i.e. the
			autoscrape row actually exists.
		-->
		<router-link v-if='catalog.autoscrape_json'
			:to='"/scraper/new/"+catalog.id' class="dropdown-item"
			style="padding:2px">Edit scraper</router-link>
		<router-link :to="'/mobile_match/'+catalog.id" class="dropdown-item" style="padding:2px"
			tt="mobile_game"></router-link>
		<router-link style="padding:2px" class="dropdown-item"
			:to="'/visual_match/'+catalog.id" tt="visual_tool"></router-link>
		<a v-if="catalog.wd_prop!=null && catalog.wd_qual==null" style="padding:2px" target='_blank'
			class="dropdown-item external"
			:href="'https://fist.toolforge.org/wdfist/?depth=3&language=en&project=wikipedia&sparql=SELECT%20?item%20WHERE%20{%20?item%20wdt:P'+catalog.wd_prop+'%20[]}&no_images_only=1&remove_used=1&remove_multiple=1&prefilled=1'"
			tt="find_images"></a>
		<a v-if="catalog.wd_prop!=null && catalog.wd_qual==null" style="padding:2px" class="dropdown-item external"
			target="_blank"
			:href="'https://wikidata-todo.toolforge.org/sparql_rc.php?sparql=SELECT+%3Fq+WHERE+%7B+%3Fq+wdt%3AP'+catalog.wd_prop+'+%5B%5D+%7D&start=last+week&end='"
			tt='changes_last_week'></a>
		<a style="padding:2px" class="dropdown-item wikidata" target='_blank'
			:href="'https://www.wikidata.org/wiki/User:Magnus_Manske/Mix%27n%27match_report/'+catalog.id"
			tt="catalog_report"></a>
		<a v-if="catalog.wd_prop!=null && catalog.wd_qual==null" style="padding:2px" class="dropdown-item wikidata"
			target="_blank" :href="'https://www.wikidata.org/wiki/Property:P'+catalog.wd_prop">P{{catalog.wd_prop}}</a>
		<div v-else class='dropdown-item' style='color:red;font-size:8pt;text-align:left;' tt='no_wd_prop'></div>
	</div>
`
};
