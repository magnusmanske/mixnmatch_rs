import { mnm_api, mnm_loading, ensure_catalog, get_specific_catalog, tt_update_interface } from './store.js';

export default Vue.extend({
    props : ['id'] ,
    data : function () { return { catalog:{} , loading:true , entries:[] , type_q:'' , limit:50 , offset:0 , min:3 , max:20 , other_cats_desc:false } } ,
    created : async function () {
    	let me = this ;
    	await ensure_catalog(me.id) ;
    	me.catalog = get_specific_catalog(me.id) ;
		if ( typeof me.$route.query.offset!='undefined' ) me.offset = me.$route.query.offset*1 ;
		if ( typeof me.$route.query.min!='undefined' ) me.min = me.$route.query.min*1 ;
		if ( typeof me.$route.query.max!='undefined' ) me.max = me.$route.query.max*1 ;
		if ( typeof me.$route.query.other_cats_desc!='undefined' ) me.other_cats_desc = me.$route.query.other_cats_desc==1 ;
		if ( typeof me.$route.query.type!='undefined' ) me.type_q = me.$route.query.type ;
		me.load_entries() ;
    },
	updated : function () { tt_update_interface() } ,
	mounted : function () { tt_update_interface() } ,
    methods : {
    	set_offset : function ( new_offset ) {
    		if ( new_offset < 0 ) new_offset = 0 ;
    		this.offset = new_offset ;
    		this.reload();
    	} ,
    	reload : function () {
    		let me = this ;
    		let url = '/common_names/' + me.id ;
    		let parts = [] ;
    		if ( me.offset != 0 ) parts.push ( "offset="+me.offset ) ;
    		if ( me.min != 3 ) parts.push ( "min="+me.min ) ;
    		if ( me.max != 20 ) parts.push ( "max="+me.max ) ;
    		if ( me.type_q != '' ) parts.push ( "type="+me.type_q ) ;
    		if ( me.other_cats_desc ) parts.push("other_cats_desc=1") ;
    		if ( parts.length > 0 ) url += "?" + parts.join("&") ;
			router.push ( url ) ;
			me.load_entries() ;
    	} ,
    	load_entries : async function () {
    		let me = this ;
	    	me.loading = true ;
	    	me.entries = [] ;
			mnm_loading(true) ;
			try {
				let d = await mnm_api('get_common_names', {
					catalog:me.id,
					limit:me.limit,
					offset:me.offset,
					min:me.min,
					max:me.max+1,
					type:me.type_q,
					other_cats_desc:me.other_cats_desc?1:0
				}) ;
				me.entries = Object.values(d.data.entries) ;
			} finally {
				me.loading = false ;
				mnm_loading(false) ;
			}
    	}
    },
    template: `
<div>
	<mnm-breadcrumb :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{tt: 'common_names'}
	]"></mnm-breadcrumb>
	<catalog-header :catalog="catalog"></catalog-header>

	<div class="card mb-3">
		<div class="card-body py-2">
			<div class="d-flex flex-wrap align-items-center gap-2">
				<div class="d-flex align-items-center gap-1">
					<small class="text-muted" style="white-space:nowrap">Other catalogs:</small>
					<input class="form-control form-control-sm" style="width:3.5rem" type="number" v-model="min" min="1" />
					<span>&ndash;</span>
					<input class="form-control form-control-sm" style="width:3.5rem" type="number" v-model="max" min="1" />
				</div>
				<label class="d-flex align-items-center gap-1 mb-0" style="cursor:pointer;font-weight:normal">
					<input type="checkbox" class="form-check-input mt-0" v-model="other_cats_desc" />
					<small tt="other_cats_desc"></small>
				</label>
				<div class="d-flex align-items-center gap-1">
					<input class="form-control form-control-sm" style="width:6rem" type="text" v-model="type_q" placeholder="Qxxx" />
					<small class="text-muted">type</small>
				</div>
				<button class="btn btn-outline-primary btn-sm" @click.prevent="reload" tt="refresh"></button>
			</div>
		</div>
	</div>

	<div v-if="loading" class="text-center py-4">
		<i tt="loading"></i>
	</div>
	<div v-else-if="entries.length==0" class="mnm-empty-state">
		<div class="mnm-empty-icon">&#x1F50D;</div>
		<p tt="no_results"></p>
	</div>
	<div v-else>
		<div class="d-flex justify-content-between align-items-center mb-2">
			<small class="text-muted">
				Showing {{offset+1}}&ndash;{{offset+entries.length}}
				<span v-if="offset>0"> &middot; Page {{Math.floor(offset/limit)+1}}</span>
			</small>
			<div class="d-flex gap-1">
				<button v-if="offset>0" class="btn btn-outline-secondary btn-sm" @click.prevent="set_offset(offset-limit)" tt="previous"></button>
				<button v-if="entries.length==limit" class="btn btn-outline-secondary btn-sm" @click.prevent="set_offset(offset+limit)" tt="next"></button>
			</div>
		</div>

		<div v-for="e in entries" :key="e.id" class="card mb-2">
			<div class="card-body py-2 px-3">
				<div class="d-flex justify-content-between align-items-baseline mb-1">
					<router-link :to="'/creation_candidates/by_ext_name/?ext_name='+encodeURIComponent(e.ext_name)" target="_blank" class="fw-bold">{{e.ext_name}}</router-link>
					<small class="text-muted text-nowrap ms-2">in {{e.cnt-1}} other catalogs</small>
				</div>
				<entry-list-item :entry="e" :show_permalink="1" :key="'eli-'+e.id"></entry-list-item>
			</div>
		</div>

		<div class="d-flex justify-content-between align-items-center mt-2">
			<small class="text-muted">
				Showing {{offset+1}}&ndash;{{offset+entries.length}}
			</small>
			<div class="d-flex gap-1">
				<button v-if="offset>0" class="btn btn-outline-secondary btn-sm" @click.prevent="set_offset(offset-limit)" tt="previous"></button>
				<button v-if="entries.length==limit" class="btn btn-outline-secondary btn-sm" @click.prevent="set_offset(offset+limit)" tt="next"></button>
			</div>
		</div>
	</div>

</div>
`
});
