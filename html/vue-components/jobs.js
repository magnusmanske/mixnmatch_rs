import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

// Every status the job runner can set, in the order we want them to appear
// as filter chips. DEACTIVATED is excluded by default — it's a "park this
// forever" state that swamps the list and isn't actionable. BLOCKED is
// filtered server-side (always hidden) so it never appears here.
const KNOWN_JOB_STATUSES = [
    'RUNNING', 'TODO', 'HIGH_PRIORITY', 'LOW_PRIORITY',
    'PAUSED', 'FAILED', 'DONE', 'DEACTIVATED'
];
const DEFAULT_HIDDEN_STATUSES = ['DEACTIVATED'];
// Statuses that are still in-flight or waiting — these jobs can be
// stopped or paused by an authorised user.
const ACTIVE_JOB_STATUSES = new Set(['RUNNING', 'TODO', 'HIGH_PRIORITY', 'LOW_PRIORITY']);

export default Vue.extend({
    props : ['id'] ,
    data : function () { return { specific_catalog:false , catalog:{} , jobs:[] , interval:'' , auto_update_job_list:false , loading:false , active:[] , stats:[], start:0, total_jobs:0, per_page:50,
        // Multi-include status filter (all-catalogs view). Default: every
        // known status except DEACTIVATED. Empty array sends no filter
        // (server returns everything).
        status_includes: KNOWN_JOB_STATUSES.filter(s => !DEFAULT_HIDDEN_STATUSES.includes(s)),
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
            // Per-catalog mode shows all statuses — the filter UI is only
            // surfaced in the "all catalogs" view where the noise really
            // bites.
            let params = { catalog:me.id, start:me.start, max:me.per_page };
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
            // Server treats `status_filter` as a comma-separated whitelist;
            // empty = no filter. Don't send when all chips are off either,
            // so users still see something rather than a blank table —
            // they can always click chips to constrain further.
            if (me.status_includes.length > 0) {
                params.status_filter = me.status_includes.join(',');
            }
            let d = await mnm_api('get_jobs', params) ;
            me.jobs = d.data ;
            me.total_jobs = d.total || 0 ;
            me.auto_update_job_list = true ;
            me.stats = d.stats;
            me.startUpdateInterval();
        },
        // Toggle a status chip. Reloading from offset 0 since the new
        // filter changes the row population — page 7 of the old filter is
        // meaningless under the new one.
        toggleStatus : function(status) {
            let i = this.status_includes.indexOf(status);
            if (i >= 0) this.status_includes.splice(i, 1);
            else this.status_includes.push(status);
            this.start = 0;
            this.load();
        },
        isStatusIncluded : function(status) {
            return this.status_includes.indexOf(status) >= 0;
        },
        statusCount : function(status) {
            // `stats` is [[name, count], ...]; we render every known status
            // as a chip even if it's not in stats, so default to 0.
            for (let i = 0; i < (this.stats || []).length; i++) {
                if (this.stats[i][0] == status) return this.stats[i][1];
            }
            return 0;
        },
        knownStatuses : function() { return KNOWN_JOB_STATUSES; },
        // Tailor the chip's "on" colour to the status's emotional weight:
        // FAILED reads as danger, DONE as success, RUNNING as warning
        // (in-flight), everything else as plain primary. "Off" chips are
        // outline-only so they fade into the background.
        statusChipClass : function(status) {
            if (!this.isStatusIncluded(status)) return 'btn-outline-secondary';
            switch (status) {
                case 'FAILED': return 'btn-danger';
                case 'DONE': return 'btn-success';
                case 'RUNNING': return 'btn-warning';
                default: return 'btn-primary';
            }
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
        // Returns true if the current user may control (stop/pause/resume) this job.
        // Mirrors the server-side is_job_manager check.
        can_control_job : function(job) {
            if (!widar || !widar.loaded || !widar.is_logged_in) return false;
            if (widar.is_catalog_admin) return true;
            if (widar.mnm_user_id && (
                (job.user_id && job.user_id == widar.mnm_user_id) ||
                job.user_name === 'automatic'
            )) return true;
            return false;
        },
        manage_job : async function(job_id, action) {
            let me = this;
            try {
                await mnm_api('manage_job', {
                    job_id: job_id,
                    action: action,
                    username: widar.getUserName()
                });
                me.load();
            } catch(e) {
                mnm_notify(e.message, 'danger');
            }
        },
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
    <h3 tt='job_list'></h3>
    <!-- Status filter chips (all-catalogs view only). Each chip toggles
         whether jobs with that status are included; counts come from
         server-side stats. DEACTIVATED is off by default. -->
    <div v-if='!specific_catalog' class='d-flex flex-wrap align-items-center gap-1 mb-2'>
        <small class='text-muted me-1'>Show statuses:</small>
        <button v-for='status in knownStatuses()' :key='status'
            type='button' class='btn btn-sm'
            :class='statusChipClass(status)'
            @click.prevent='toggleStatus(status)'
            :title='isStatusIncluded(status) ? "Click to hide" : "Click to show"'>
            {{status.replace(/_/g,' ').toLowerCase()}}
            <span class='badge text-bg-light ms-1'>{{statusCount(status)}}</span>
        </button>
    </div>

    <!-- Pagination -->
    <pagination v-if='total_jobs > per_page' :offset='start' :items-per-page='per_page' :total='total_jobs'
        :show-first-last='true' @go-to-page='goToOffset'></pagination>

    <div class='table-responsive'>
    <table class='table' id='jobs_table' style='font-size:90%'>
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
        <tbody v-for='job in jobs' :key='"g-"+job.id' class='jobs-group'>
            <tr class='jobs-main'>
                <td v-if='!specific_catalog'>
                    <span v-if='!job.catalog' class='text-muted fst-italic' title='Job is not tied to a specific catalog'>(global)</span>
                    <template v-else>
                        <router-link :to='"/jobs/"+job.catalog'>{{job.catalog}}</router-link>
                        <div class='jobs-catalog-name'>{{job.catalog_name}}</div>
                    </template>
                </td>
                <td class='jobs-action'>{{job.action.replace(/_/g,' ')}}</td>
                <td>
                    <span :class='"jobs-"+job.status.toLowerCase()'>{{job.status}}</span>
                    <span v-if='can_control_job(job)' class='jobs-controls ms-1'>
                        <button v-if='job.status==="PAUSED" || job.status==="DEACTIVATED"'
                            class='jobs-ctrl-btn btn btn-sm btn-outline-success'
                            title='Resume job' @click.prevent='manage_job(job.id,"resume")'>&#x25B6;</button>
                        <button v-if='job.status!=="PAUSED" && job.status!=="DONE" && job.status!=="FAILED" && job.status!=="DEACTIVATED"'
                            class='jobs-ctrl-btn btn btn-sm btn-outline-warning'
                            title='Pause job' @click.prevent='manage_job(job.id,"pause")'>&#x23F8;</button>
                        <button v-if='job.status!=="DONE" && job.status!=="FAILED" && job.status!=="DEACTIVATED"'
                            class='jobs-ctrl-btn btn btn-sm btn-outline-danger'
                            title='Stop job' @click.prevent='manage_job(job.id,"stop")'>&#x23F9;</button>
                    </span>
                </td>
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
            <tr v-if='job.note' class='jobs-note'>
                <td :colspan='main_colspan()'>
                    <small class='text-muted jobs-note-text'>
                        <span class='fw-semibold me-1'>Note:</span>{{job.note}}
                    </small>
                </td>
            </tr>
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
