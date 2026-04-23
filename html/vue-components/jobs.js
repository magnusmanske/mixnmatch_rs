(function() {
  const s = document.createElement('style');
  s.textContent = `
.jobs-todo { color:#36c !important; }
.jobs-done { color:#14866d !important; }
.jobs-running { color:#fc3 !important; }
.jobs-failed { color:#b32424 !important; }
.jobs-low_priority, .jobs-high_priority { color:#36c !important; font-style:italic; }
/* Job rows: each job is one main row plus an optional note row.
   Use a top border between job groups instead of table-striped — the
   variable row count per group breaks alternating-row striping. */
#jobs_table { table-layout: auto; }
#jobs_table tbody tr.jobs-main > td { border-top: 1px solid #dee2e6; vertical-align: top; }
#jobs_table tbody tr.jobs-main:first-child > td { border-top: 0; }
#jobs_table tbody tr.jobs-note > td {
  border-top: 0;
  padding-top: 0;
  padding-bottom: .55rem;
  padding-left: 1.25rem;
}
#jobs_table .jobs-ts { line-height: 1.15; }
#jobs_table .jobs-ts-date { display: block; }
#jobs_table .jobs-ts-time { display: block; color:#6c757d; font-size: .75rem; }
#jobs_table .jobs-action { word-break: break-word; }
#jobs_table .jobs-note-text { word-break: break-word; }
#jobs_table .jobs-catalog-name { font-size: .75rem; line-height: 1.1; color:#6c757d; }
@media (max-width: 575.98px) {
  /* On phones, hide the rarely-set "recurring" column to win width back. */
  #jobs_table .jobs-col-recurring { display: none; }
}
`;
  document.head.appendChild(s);
})();

import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

export default Vue.extend({
    props : ['id'] ,
    data : function () { return { specific_catalog:false , catalog:{} , jobs:[] , interval:'' , auto_update_job_list:false , loading:false , active:[] , stats:[], start:0, total_jobs:0, per_page:50, status_filter:'',
        valid_actions:['automatch','automatch_by_search','automatch_from_other_catalogs','automatch_by_sitelink','automatch_people_with_birth_year','purge_automatches','auxiliary_matcher','taxon_matcher'] ,
    } } ,
    beforeDestroy : function () {
        let me = this ;
        if ( me.interval != '' ) {
            clearInterval(me.interval);
            me.interval = '' ;
        }
    } ,
    created : async function () {
        let me = this ;
        me.specific_catalog = typeof me.id != 'undefined' ;
        if ( me.specific_catalog ) {
            await ensure_catalog(me.id) ;
            me.catalog = get_specific_catalog(me.id) || me.get_catalog(me.id) ;
            // Gate the generic automatchers on the catalog's opt-out flag.
            // `no_automatches` is the legacy PHP-style signal; kv_pairs is
            // the new source of truth — check both.
            const kv = me.catalog.kv_pairs || {};
            if ( me.catalog.no_automatches=='1' || kv.use_automatchers=='0' ) {
                me.valid_actions = me.valid_actions.filter((action) => !action.includes('automatch'));
            }
            if ( me.catalog.has_person_date == 'yes' ) {
                me.valid_actions.push('match_person_dates');
                me.valid_actions.push('match_on_birthdate');
                me.valid_actions.push('match_on_deathdate');
            }
            if ( me.catalog.has_autoscrape == 1 ) me.valid_actions.push('autoscrape');
            // Offer the SPARQL-driven automatch only when a catalog admin
            // has actually configured a query via kv_catalog.
            if ( me.catalog.kv_pairs && typeof me.catalog.kv_pairs.automatch_sparql != 'undefined' ) {
                me.valid_actions.push('automatch_sparql');
            }
            // Same for the property-root-driven automatch_complex.
            if ( me.catalog.kv_pairs && typeof me.catalog.kv_pairs.automatch_complex != 'undefined' ) {
                me.valid_actions.push('automatch_complex');
            }
        }
        me.load();
    },
	updated : function () { tt_update_interface() } ,
	mounted : function () { tt_update_interface() } ,
    methods : {
        load : async function() {
            let me = this ;
            if ( me.loading ) return ;
            me.loading = true ;
            let el = document.getElementById('jobs_table');
            if (el) el.classList.add('text-muted');
            try {
                if ( me.specific_catalog ) await me.loadSpecifcCatalog() ;
                else await me.loadAllCatalogs() ;
            } finally {
                me.loading = false ;
                if (el) el.classList.remove('text-muted');
            }
        },
        startUpdateInterval : function() {
            let me = this ;
            if ( me.interval != '' ) {
                clearInterval(me.interval);
                me.interval = '' ;
            }
            if ( me.auto_update_job_list ) {
                me.interval = setInterval(function(){ if (!me.loading) me.load(); },1000*30) ; // 30sec
            }
        } ,
        loadSpecifcCatalog : async function() {
            let me = this ;
            let params = { catalog:me.id, start:me.start, max:me.per_page };
            if (me.status_filter) params.status_filter = me.status_filter;
            let d = await mnm_api('get_jobs', params) ;
            me.active = [] ;
            me.auto_update_job_list = false ;
            (d.data||[]).forEach ( function ( job ) {
                if ( job.status == 'TODO' || job.status == 'RUNNING' || job.status == 'LOW_PRIORITY' || job.status == 'HIGH_PRIORITY' ) {
                    me.auto_update_job_list = true ;
                    me.active[job.action] = 1 ;
                }
            } ) ;
            me.jobs = d.data ;
            me.total_jobs = d.total || 0 ;
            me.startUpdateInterval();
        },
        loadAllCatalogs : async function() {
            let me = this ;
            let params = { start:me.start, max:me.per_page };
            if (me.status_filter) params.status_filter = me.status_filter;
            let d = await mnm_api('get_jobs', params) ;
            me.jobs = d.data ;
            me.total_jobs = d.total || 0 ;
            me.auto_update_job_list = true ;
            me.stats = d.stats;
            me.startUpdateInterval();
        },
        get_catalog : function ( catalog_id ) {
            let dummy = {name:"No such catalog",is_fake:true};
            try {
                let ret = get_specific_catalog(catalog_id) ;
                if ( typeof ret=='undefined' ) return dummy;
                return ret;
            } catch(err) {
                return dummy;
            }
        } ,
        start_new_job : async function ( action ) {
            let me = this ;
            try {
                await mnm_api('start_new_job', {
                    catalog:me.id,
                    action:action,
                    username:widar.getUserName()
                }) ;
                me.load(); // Referesh
            } catch (e) {
                mnm_notify(e.message, 'danger') ;
            }
        } ,
        format_recurring : function(sec) {
            if ( typeof sec=='undefined' || sec==null || sec=='' || sec*1==0 ) return '' ;
            if ( sec < 3600 ) return sec+'s' ;
            if ( sec >= 3600 && sec < 60*60*24 ) return (sec/3600).toFixed(1)+'h' ;
            if ( sec >= 60*60*24 && sec < 60*60*24*30 ) return (sec/3600).toFixed(1)+'days' ;
            return '~'+(sec/(60*60*24*30)).toFixed(1)+' months' ;
        } ,
        format_ts : function(ts) {
            if ( typeof ts == 'undefined' ) return '' ;
            if ( ts == null ) return '' ;
            if ( ts == '' ) return '';
            return ts.substr(0,4)+'-'+ts.substr(4,2)+'-'+ts.substr(6,2)+' '+ts.substr(8,2)+':'+ts.substr(10,2)+':'+ts.substr(12,2);
        },
        // Split a timestamp into its date/time halves so the cell can stack
        // them vertically — saves ~60px per timestamp column vs. the old
        // `nowrap` "yyyy-mm-dd hh:mm:ss" single line.
        date_part : function(ts) {
            const s = this.format_ts(ts);
            return s ? s.substr(0,10) : '';
        },
        time_part : function(ts) {
            const s = this.format_ts(ts);
            return s ? s.substr(11) : '';
        },
        // Total number of columns in the main row, used as colspan for the
        // full-width note row below each job.
        main_colspan : function() {
            return this.specific_catalog ? 6 : 7;
        },
        goToOffset: function(new_offset) {
            this.start = new_offset;
            this.load();
            if (typeof window != 'undefined' && window.scrollTo) window.scrollTo(0, 0);
        },
        applyStatusFilter: function() {
            this.start = 0;
            this.load();
        }
    },
    template: `
<div class='mt-2'>
	<mnm-breadcrumb v-if='specific_catalog && catalog && catalog.id' :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{text: 'Jobs'}
	]"></mnm-breadcrumb>
	<mnm-breadcrumb v-else :crumbs="[{text: 'Jobs'}]"></mnm-breadcrumb>
	<catalog-header v-if='specific_catalog' :catalog="catalog"></catalog-header>
    <h1 v-else tt='all_catalogs'></h1>
    <div v-if='specific_catalog'>
        <h3 tt='start_new_job' style='vertical-align:top'></h3>
        <table class='table'>
            <tbody>
                <tr v-if='catalog.no_automatches=="1"'>
                    <td/>
                    <td><i tt='no_automatches'></i></td>
                </tr>
                <tr v-for='action in valid_actions' style='margin-right:0.5em'>
                    <td nowrap>
                        <span v-if='typeof active[action]!="undefined"'>{{action.replace(/_/g,' ')}}</span>
                        <a v-else href='#' @click.prevent='start_new_job(action)'>{{action.replace(/_/g,' ')}}</a>
                    </td>
                    <td>
                        <span v-if='action=="automatch_sparql"'>
                            Runs the catalog's configured SPARQL query against Wikidata and matches entries by the returned IDs.
                            Recommended: run <em>purge automatches</em> first so re-runs aren't blocked by stale preliminary matches.
                        </span>
                        <span v-else-if='action=="automatch_complex"'>
                            Runs a multi-property SPARQL match using the catalog's configured property roots, then confirms candidates via Wikidata search.
                            Recommended: run <em>purge automatches</em> first so re-runs aren't blocked by stale preliminary matches.
                        </span>
                        <span v-else :tt='"snj_"+action'></span>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
    <div v-else style="float: right;">
        <span v-for='s in stats' class="badge text-bg-light">
            <a href='#' @click.prevent='status_filter=(status_filter==s[0]?"":s[0]);applyStatusFilter()' :style='status_filter==s[0]?"font-weight:bold":""'>
                {{s[0]}}: {{s[1]}}
            </a>
        </span>&nbsp;
    </div>
    <h3 tt='job_list'></h3>
    <div v-if='status_filter' class='mb-2'>
        <span class='badge text-bg-primary'>{{status_filter}}
            <a href='#' @click.prevent='status_filter="";applyStatusFilter()' style='color:white;text-decoration:none;margin-left:4px'>&times;</a>
        </span>
    </div>

    <!-- Pagination -->
    <pagination v-if='total_jobs > per_page' :offset='start' :items-per-page='per_page' :total='total_jobs'
        :show-first-last='true' @go-to-page='goToOffset'></pagination>

    <div class='table-responsive'>
    <table class='table table-hover' id='jobs_table' style='font-size:90%'>
        <thead>
            <tr>
                <th v-if='!specific_catalog' tt='catalog' style='min-width:7rem;'></th>
                <th tt='actions'></th>
                <th tt='status'></th>
                <th tt='last_change'></th>
                <th class='jobs-col-recurring' tt='recurring'></th>
                <th tt='next_scheduled_run'></th>
                <th tt='user'></th>
            </tr>
        </thead>
        <tbody>
            <template v-for='job in jobs'>
                <tr class='jobs-main' :key='"j-"+job.id'>
                    <td v-if='!specific_catalog'>
                        <router-link :to='"/jobs/"+job.catalog'>{{job.catalog}}</router-link>
                        <div class='jobs-catalog-name'>{{job.catalog_name}}</div>
                    </td>
                    <td class='jobs-action'>{{job.action.replace(/_/g,' ')}}</td>
                    <td><span :class='"jobs-"+job.status.toLowerCase()'>{{job.status}}</span></td>
                    <td class='jobs-ts'>
                        <span class='jobs-ts-date'>{{date_part(job.last_ts)}}</span>
                        <span class='jobs-ts-time'>{{time_part(job.last_ts)}}</span>
                    </td>
                    <td class='jobs-col-recurring'>{{format_recurring(job.repeat_after_sec)}}</td>
                    <td class='jobs-ts'>
                        <span class='jobs-ts-date'>{{date_part(job.next_ts)}}</span>
                        <span class='jobs-ts-time'>{{time_part(job.next_ts)}}</span>
                    </td>
                    <td>
                        <userlink v-if='job.user_id!=0' :username='job.user_name' :user_id='job.user_id' />
                        <span v-else>{{job.user_name}}</span>
                    </td>
                </tr>
                <tr v-if='job.note' class='jobs-note' :key='"n-"+job.id'>
                    <td :colspan='main_colspan()'>
                        <small class='text-muted jobs-note-text'>
                            <span class='fw-semibold me-1'>Note:</span>{{job.note}}
                        </small>
                    </td>
                </tr>
            </template>
        </tbody>
    </table>
    </div>

    <!-- Bottom pagination -->
    <pagination v-if='total_jobs > per_page' :offset='start' :items-per-page='per_page' :total='total_jobs'
        @go-to-page='goToOffset'></pagination>

    <div class='d-flex align-items-center gap-2'>
        <button class='btn btn-outline-primary btn-sm' @click.prevent='load()' tt='refresh'></button>
        <label v-if='specific_catalog' class='form-check-label' style='cursor:pointer'>
            <input type='checkbox' class='form-check-input' v-model='auto_update_job_list' @change='startUpdateInterval' />
            <small tt='jobs_auto_updating'></small>
        </label>
    </div>
</div>
`
});
