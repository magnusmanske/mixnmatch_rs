(function() {
  const s = document.createElement('style');
  s.textContent = `
	div.section {
		margin-bottom: 0.5rem;
	}

	div.section span {
		margin-left: 0.1rem;
		margin-right: 0.2rem;
	}
`;
  document.head.appendChild(s);
})();

import { mnm_api, tt_update_interface } from './store.js';

let entries_page_default_values = {
	catalogs_yes: '', catalogs_no: '', entry_types: '',
	unmatched: true, prelim_matched: true, fully_matched: false,
	aux: [{ "property": "", "value": "" }],
	has_birth_date: 'any', birth_year_before: '', birth_year_after: '',
	has_death_date: 'any', death_year_before: '', death_year_after: '',
	has_location: 'any',
	given_name: '', given_name_gender: 'any',
	offset: 0, batch_size: 25
};

export default Vue.extend({
	props: ['gender'],
	data: function () {
		let ret = JSON.parse(JSON.stringify(entries_page_default_values));
		ret.loading = false;
		ret.loaded = false;
		ret.entries = {};
		return ret
	},
	created: function () {
		let me = this;
		let autoload = false;
		Object.keys(entries_page_default_values).forEach((key, index) => {
			if (typeof me.$route.query[key] == 'undefined') return;
			if (me.$route.query[key] == me[key]) return;
			if (Array.isArray(me[key])) me[key] = JSON.parse(me.$route.query[key]);
			else me[key] = me.$route.query[key];
			autoload = true;
			//console.log(key,me.$route.query[key]);
		});
		if (autoload) me.load_entries();
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		add_aux: function () {
			this.aux.push({ "property": "", "value": "" });
		},
		remove_aux: function (k) {
			this.aux.splice(k, 1);
		},
		nearby_pages: function (front, back) {
			let me = this;
			let ret = [];
			let current_page = me.offset / me.batch_size;
			for (let page = current_page - front; page <= current_page + back; page++) {
				if (page >= 0 && page != current_page) ret.push(page);
			}
			return ret;
		},
		run: function () {
			this.load_offset(0);
		},
		update_params: function () {
			let me = this;
			let url = '/entries?';
			Object.keys(entries_page_default_values).forEach((key, index) => {
				if (JSON.stringify(me[key]) == JSON.stringify(entries_page_default_values[key])) return;
				let v = '';
				if (Array.isArray(me[key])) v = JSON.stringify(me[key]);
				else v = me[key];
				url += "&" + key + '=' + v;
			});
			router.push(url);
		},
		load_offset: function (new_offset) {
			let self = this;
			self.offset = new_offset;
			this.update_params();
			self.load_entries();
		},
		load_entries: function () {
			let me = this;
			me.loading = true;
			me.entries = [];
			let params = {};
			Object.keys(entries_page_default_values).forEach((key, index) => {
				if (Array.isArray(me[key])) params[key] = JSON.stringify(me[key]);
				//else if ( Object.isObject(me[key]) ) params[key] = JSON.stringify(me[key]) ;
				else if (me[key] === true) params[key] = 1;
				else if (me[key] === false) params[key] = 0;
				else params[key] = '' + me[key];
			});
			//console.log(JSON.parse(JSON.stringify(params)))
			mnm_api('entries_query', params).then(function (d) {
				//console.log(JSON.parse(JSON.stringify(d)))
				me.loaded = true;
				me.entries = d.data.entries;
				me.loading = false;
			});
		}
	},
	template: `
<div class='mt-2 entries-page'>
	<mnm-breadcrumb :crumbs="[{text: 'Entries'}]"></mnm-breadcrumb>
	<p tt='entries_page_blurb'></p>
	<div class='container-fluid'>
		<div class='section'>
			<div class='row'>
				<div class='col-4' tt='catalogs_yes'></div>
				<div class='col-8'><input type='text' v-model='catalogs_yes' style='width: 100%' tt_placeholder='comma_separated' /></div>
			</div>
			<div class='row'>
				<div class='col-4' tt='catalogs_no'></div>
				<div class='col-8'><input type='text' v-model='catalogs_no' style='width: 100%' tt_placeholder='comma_separated' /></div>
			</div>
			<div class='row'>
				<div class='col-4' tt='entry_types2'></div>
				<div class='col-8'><input type='text' v-model='entry_types' style='width: 100%' tt_placeholder='comma_separated' /></div>
			</div>
			<div class='row'>
				<div class='col-4' tt='status'></div>
				<div class='col-8'>
					<label><input type='checkbox' v-model='unmatched' /><span tt='unmatched'></span></label>
					<label><input type='checkbox' v-model='prelim_matched' /><span tt='auto_matched'></span></label>
					<label><input type='checkbox' v-model='fully_matched' /><span tt='manually_matched'></span></label>
				</div>
			</div>
			<div class='row'>
				<div class='col-4' tt='aux_data'></div>
				<div class='col-8'>
					<div v-for='v,k in aux' style='display:flex'>
						<div style='width:100%;display:flex'>
							<input type='text' v-model='v.property' tt_placeholder='property' style='width: 7rem' />
							<input type='text' v-model='v.value' tt_placeholder='aux_value' style='width: 100%' />
						</div>
						<button class='btn btn-outline-danger' @click.prevent='remove_aux(k)' tt='remove'></button>
					</div>
					<div>
						<button class='btn btn-outline-success' @click.prevent='add_aux'>+</button>
					</div>
				</div>
			</div>
			<div class='row'>
				<div class='col-4' tt='has_location'></div>
				<div class='col-8'>
					<div style='display:flex'>
						<label><input type='radio' v-model='has_location' value='yes' /><span tt='yes'></span></label>
						<label><input type='radio' v-model='has_location' value='no' /><span tt='no'></span></label>
						<label><input type='radio' v-model='has_location' value='any' /><span tt='any'></span></label>
					</div>
				</div>
			</div>
			<div class='row'>
				<div class='col-4' tt='birth_date'></div>
				<div class='col-8'>
					<div style='display:flex'>
						<label><input type='radio' v-model='has_birth_date' value='yes' /><span tt='yes'></span></label>
						<label><input type='radio' v-model='has_birth_date' value='no' /><span tt='no'></span></label>
						<label><input type='radio' v-model='has_birth_date' value='any' /><span tt='any'></span></label>
						<span v-if='has_birth_date=="yes"' style='margin-left:2rem;'>
							<input type='string' v-model='birth_year_after' style='width: 5rem;' tt_placeholder='zero_prefixed_year' />
							&nbsp;&lt;&nbsp;<span tt='year'></span>&nbsp;&lt;&nbsp;
							<input type='string' v-model='birth_year_before' style='width: 5rem;' tt_placeholder='zero_prefixed_year' />
						</span>
					</div>
				</div>
			</div>
			<div class='row'>
				<div class='col-4' tt='death_date'></div>
				<div class='col-8'>
					<div style='display:flex'>
						<label><input type='radio' v-model='has_death_date' value='yes' /><span tt='yes'></span></label>
						<label><input type='radio' v-model='has_death_date' value='no' /><span tt='no'></span></label>
						<label><input type='radio' v-model='has_death_date' value='any' /><span tt='any'></span></label>
						<span v-if='has_death_date=="yes"' style='margin-left:2rem;'>
							<input type='string' v-model='death_year_after' style='width: 5rem;' tt_placeholder='zero_prefixed_year' />
							&nbsp;&lt;&nbsp;<span tt='year'></span>&nbsp;&lt;&nbsp;
							<input type='string' v-model='death_year_before' style='width: 5rem;' tt_placeholder='zero_prefixed_year' />
						</span>
					</div>
				</div>
			</div>
			<div class='row'>
				<div class='col-4' tt='has_given_name'></div>
				<div class='col-8'>
					<div style='display:flex'>
						<label><input type='radio' v-model='given_name_gender' value='any' /><span tt='any'></span></label>
						<label><input type='radio' v-model='given_name_gender' value='yes' /><span tt='yes'></span></label>
						<label><input type='radio' v-model='given_name_gender' value='no' /><span tt='no'></span></label>
						<label><input type='radio' v-model='given_name_gender' value='unknown' /><span tt='gender_unknown'></span></label>
						<label><input type='radio' v-model='given_name_gender' value='ambiguous' /><span tt='gender_ambiguous'></span></label>
						<label><input type='radio' v-model='given_name_gender' value='female' /><span tt='gender_female'></span></label>
						<label><input type='radio' v-model='given_name_gender' value='male' /><span tt='gender_male'></span></label>
					</div>
					<div>
						<input type='text' v-model='given_name' tt_placeholder='specific_given_name' />
					</div>
				</div>
			</div>
			<!--
				TODO
				auth_control_gender
				entry_creation
				kv_entry
				top_missing_groups (to fill catalogs_yes)
				matches by user
			-->
			<div class='row'>
				<button class='btn btn-outline-primary' tt='run' @click.prevent='run'></button>
			</div>
		</div>
	</div>
	<div v-if='loading'>
		<i tt='loading'></i>
	</div>
	<div v-else>

		<div v-if='Object.keys(entries).length>0'>
			<nav aria-label="Page navigation">
			  <ul class="pagination justify-content-center">
			    <li v-if='offset>0' class="page-item">
			      <a class="page-link" href="#" aria-label="Previous" @click.prevent='load_offset(offset*1-batch_size)'>
			        <span aria-hidden="true">&laquo;</span>
			      </a>
			    </li>
			    <li class="page-item" v-if='offset/batch_size>4'>
			    	<a class="page-link" href="#" @click.prevent='load_offset(0)'>1</a>
			    </li>
			    <li class='page-item disabled' v-if='offset/batch_size>5'>
      			<a class="page-link">...</a>
			    </li>
			    <li class="page-item" v-for='page in nearby_pages(4,0)'>
			    	<a class="page-link" href="#" @click.prevent='load_offset(page*batch_size)'>{{page+1}}</a>
			    </li>
			    <li class='page-item disabled'>
      			<a class="page-link">{{1+(offset)/batch_size}}</a>
			    </li>
			    <li class="page-item" v-for='page in nearby_pages(0,4)'>
			    	<a class="page-link" href="#" @click.prevent='load_offset(page*batch_size)'>{{page+1}}</a>
			    </li>
			    <li class="page-item">
			      <a class="page-link" href="#" aria-label="Next" @click.prevent='load_offset(offset*1+batch_size)'>
			        <span aria-hidden="true">&raquo;</span>
			      </a>
			    </li>
			  </ul>
			</nav>

			<entry-list-item v-for="e in entries" :entry="e" :show_catalog="1" :show_permalink="1" :twoline="1" key="e.id"></entry-list-item>
		</div>
		<div v-else>
			<i v-if='loaded' tt='no_matches'></i>
		</div>
	</div>

</div>
`
});
