import { mnm_api, mnm_notify, ensure_catalogs, get_specific_catalog, tt_update_interface, widar } from './store.js';

export default Vue.extend({
    props : ['type','initial_catalogs'] ,
    data : function () { return { limit:10 , issues:[] , num_issues:0 , start:0 , available_types:['WD_DUPLICATE','MISMATCH','MISMATCH_DATES','MULTIPLE'] , total:-1 , catalogs:'' , loading:false } } ,
    created : function () {
    	if ( typeof this.initial_catalogs != 'undefined' ) this.catalogs = this.initial_catalogs ;
    	this.load();
    },
	updated : function () { tt_update_interface() } ,
	mounted : function () { tt_update_interface() } ,
    methods : {
    	load : async function () {
    		let me = this ;
    		me.issues = [] ;
    		me.loading = true ;
    		let the_type = (me.type||'') ;
    		if ( the_type == 'ALL' ) the_type = '' ;
    		let d = await mnm_api('get_issues', {
    			type : the_type,
    			limit : me.limit ,
    			offset : (me.start||0) ,
    			catalogs : me.catalogs
    		}) ;
    		//console.log(JSON.parse(JSON.stringify(d)));
    		let path = '/issues' ;
    		if ( the_type!='' || me.catalogs!='' ) path += '/'+(the_type||'ALL')+me.get_catalog_slash() ;
    		//me.$router.replace ( path );
			Object.entries ( d.data.entries ).forEach ( function ( [k , v] ) {
				if ( typeof d.data.users[v.user] == 'undefined' ) return ;
				d.data.entries[k].username = d.data.users[v.user].name ;
			} ) ;
			// Ensure all referenced catalogs are cached
			var catalog_ids = [...new Set(Object.values(d.data.entries).map(function (e) { return e.catalog; }))];
			await ensure_catalogs(catalog_ids);
			me.total = d.data.open_issues*1 ;
    		me.entries = d.data.entries ;
    		me.issues = Array.isArray(d.data.issues) ? d.data.issues : Object.values(d.data.issues || {}) ;
    		me.num_issues = 0 ;
    		me.issues.forEach ( function ( v , k ) {
    			Vue.set ( me.issues[k] , 'is_resolved' , false ) ;
    			me.num_issues++ ;
    		} ) ;
    		me.loading = false ;
    	} ,
    	canResolve : function () {
    		return typeof widar.getUserName() != 'undefined'
    	} ,
    	get_catalog : function ( catalog_id ) {
    		return get_specific_catalog(catalog_id);
    	} ,
    	get_catalog_slash : function () {
    		if ( this.catalogs == '' ) return '' ;
    		return '/'+this.catalogs ;
    	} ,
    	resolve : async function ( issue_id ) {
    		let me = this ;
    		try {
    			await mnm_api('resolve_issue', {
    				issue_id : issue_id ,
    				username : widar.getUserName()
    			}) ;
    			Vue.set ( me.issues[issue_id] , 'is_resolved' , true ) ;
    			me.total-- ;
    		} catch (e) {
    			mnm_notify(e.message, 'danger') ;
    		}
    	}
    },
    template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{tt: 'issues'}]"></mnm-breadcrumb>
	<h1 tt='issues'></h1>
	<div style='float:right'>
		<button class='btn btn-outline-secondary' tt='reload' @click.prevent='load'></button>
	</div>
	<p tt='issues_blurb'></p>
	<p>
		<span tt='open_issues'></span>:
		<span v-if='total>-1'>
			{{total}}
		</span>
	</p>
	<p>
		<span tt='catalogs'></span>
		<input type='text' v-model='catalogs' />
	</p>
	<div style="border-bottom: 1px solid #DDD;margin-bottom: 0.2rem;">
		<span>
			<span v-if='typeof type=="undefined"||type==""||type=="ALL"' tt='all_types'></span>
			<router-link v-else :to='"/issues/ALL"+get_catalog_slash()' tt='all_types'></router-link>
		</span>
		<span v-for='possible_type in available_types'>
			|
			<span v-if='type==possible_type' :tt='possible_type.toLowerCase()'></span>
			<router-link v-else :to='"/issues/"+possible_type+get_catalog_slash()' :tt='possible_type.toLowerCase()'></router-link>
		</span>
	</div>
	<div v-if='loading'>
		<i tt='loading'></i>
	</div>
	<div v-else-if='num_issues==0'>
		<i tt='no_results'></i>
	</div>
	<div v-else v-for='(i,i_idx) in issues' class='card' :key='i.id' style='margin-bottom:1rem;'>
		<div class="card-body">
			<entry-list-item :show_catalog=1 :entry="entries[i.entry_id]" :show_permalink="1" :key="i.entry_id"></entry-list-item>
			<div style='display: flex;flex-direction: row;'>
				<div v-if='i.is_resolved' tt='resolved'>
				</div>
				<div v-else-if='i.type=="MISMATCH"' style='flex-grow: 1;'>
					<h3 tt='mismatch'></h3>
					<span v-for='(q,q_idx) in i.json'>
						<span v-if='q_idx>0'>, </span>
						<wd-link :item='q.replace(/^\\{/,"")'></wd-link>
					</span>
				</div>
				<div v-else-if='i.type=="MISMATCH_DATES"' style='flex-grow: 1;'>
					<h3 tt='mismatch_dates'></h3>
					<table>
						<tr>
							<th tt='toolname'></th>
							<td>{{i.json.mnm_time.replace(/T.*\$/,'').replace(/\\+/,'')}}</td>
						</tr>
						<tr>
							<th tt='wikidata'></th>
							<td>{{i.json.wd_time.replace(/T.*\$/,'').replace(/\\+/,'')}}</td>
						</tr>
					</table>
				</div>
				<div v-else-if='i.type=="WD_DUPLICATE"' style='flex-grow: 1;'>
					<h3 tt='wd_duplicate'></h3>
					<span v-for='(q,q_idx) in i.json'>
						<span v-if='q_idx>0'>, </span>
						<wd-link :item='q'></wd-link>
					</span>
				</div>
				<div v-else-if='i.type=="MULTIPLE"' style='flex-grow: 1;'>
					<h3 tt='multiple'></h3>
					<div><small tt='multiple_desc'></small></div>
					<table>
						<tr v-if='get_catalog(entries[i.entry_id].catalog) && get_catalog(entries[i.entry_id].catalog).wd_prop!=null && get_catalog(entries[i.entry_id].catalog).wd_qual==null'>
							<th tt='property'></th>
							<td>
								<wd-link :item='"P"+get_catalog(entries[i.entry_id].catalog).wd_prop'></wd-link>
							</td>
						</tr>
						<tr>
							<th tt='wikidata'></th>
							<td>{{i.json.wd.join(', ')}}</td>
						</tr>
						<tr>
							<th tt='toolname'></th>
							<td>{{i.json.mnm}}</td>
						</tr>
					</table>
				</div>
				<div v-else style='flex-grow: 1;'>
					<h3>{{i.type}}</h3>
					<pre>
						{{i.json}}
					</pre>
				</div>
				<div v-if='!i.is_resolved'>
					<button v-if='canResolve()' class='btn btn-outline-primary' tt='resolve' @click.prevent='resolve(i.id)'></button>
					<span v-else tt='log_into_widar'></span>
				</div>
			</div>
		</div>
	</div>
</div>
`
});
