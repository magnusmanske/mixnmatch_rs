import { tt_update_interface } from './store.js';

export default Vue.extend({
	props: ['catalogs'],
	data: function () {
		return {
			format: 'tab', as_file: 1,
			columns: { exturl: 1, username: 1, aux: 0, dates: 0, location: 0, multimatch: 1 },
			hidden: { any_matched: 0, firmly_matched: 0, user_matched: 0, unmatched: 0, automatched: 0, name_date_matched: 0, aux_matched: 0, no_multiple: 0 },
			ext_ids: { P214: 0, P227: 0 }
		}
	},
	created: function () { this.init(); },
	updated: function () { tt_update_interface() }, //  $('.next_cc_set').focus() ;
	mounted: function () { tt_update_interface(); var _ncc = document.querySelector('.next_cc_set'); if (_ncc) _ncc.focus(); },
	methods: {
		init: function () {
			const me = this;
		},
		generateDownloadURL: function () {
			const me = this;
			Object.keys(me.columns).forEach(function (k) { me.columns[k] = me.columns[k] ? 1 : 0; });
			Object.keys(me.hidden).forEach(function (k) { me.hidden[k] = me.hidden[k] ? 1 : 0; });
			Object.keys(me.ext_ids).forEach(function (k) { me.ext_ids[k] = me.ext_ids[k] ? 1 : 0; });
			var url = '/api.php?query=download2';
			url += '&catalogs=' + me.catalogs.replace(/[^0-9,]/g, '');
			url += '&columns=' + encodeURIComponent(JSON.stringify(me.columns));
			url += '&hidden=' + encodeURIComponent(JSON.stringify(me.hidden));
			var active_ext_ids = [];
			Object.entries(me.ext_ids).forEach(function ([k, v]) { if (v) active_ext_ids.push(k); });
			if (active_ext_ids.length > 0) url += '&ext_ids=' + encodeURIComponent(active_ext_ids.join(','));
			url += "&format=" + me.format;
			if (me.as_file != false) url += "&as_file=1";
			return url;
		}
	},
	template: `
	<div>
		<mnm-breadcrumb :crumbs="[{tt: 'download_page'}]"></mnm-breadcrumb>
		<h2 tt='download_page'></h2>
		<form>

			<div class="mb-3 row">
				<label class="col-sm-2 col-form-label" tt="catalogs"></label>
				<div class="col-sm-10">
					<input type='text' class='form-control' v-model='catalogs' tt_placeholder='ph_catalogs' />
				</div>
			</div>

			<div class="mb-3 row">
				<label class="col-sm-2 col-form-label" tt="columns"></label>
				<div class="col-sm-10">
					<div v-for='(value,colname) in columns' class='row'>
						<label>
							<input type='checkbox' v-model='columns[colname]' value='1' />
							<span :tt='"col_"+colname'></span>
						</label>
					</div>
				</div>
			</div>

			<div class="mb-3 row">
				<label class="col-sm-2 col-form-label" tt="hide_rows"></label>
				<div class="col-sm-10">
					<div v-for='(value,name) in hidden' class='row'>
						<label>
							<input type='checkbox' v-model='hidden[name]' value='1' />
							<span :tt='"hide_row_"+name'></span>
						</label>
					</div>
				</div>
			</div>

			<div class="mb-3 row">
				<label class="col-sm-2 col-form-label" tt="ext_ids_section"></label>
				<div class="col-sm-10">
					<div v-for='(value,prop) in ext_ids' class='row'>
						<label>
							<input type='checkbox' v-model='ext_ids[prop]' value='1' />
							<wd-link :item='prop' :key='"ext_id_"+prop' as_text="1" />
						</label>
					</div>
				</div>
			</div>

			<div class="mb-3 row">
				<label class="col-sm-2 col-form-label" tt="format"></label>
				<div class="col-sm-10">
					<div class='row'>
						<label v-for='(format_option) in ["tab","json"]' style='margin-right:20px;'>
							<input type='radio' v-model='format' :value='format_option' />
							<span :tt='"download_as_"+format_option'></span>
							<small v-if='format_option=="tab"' class='text-muted'> (TSV, spreadsheet-friendly)</small>
							<small v-if='format_option=="json"' class='text-muted'> (structured, for scripts)</small>
						</label>
					</div>
					<div class='row'>
						<label>
							<input type='checkbox' v-model='as_file' value='1' />
							<span tt='download_as_file'></span>
						</label>
					</div>
				</div>
			</div>

		</form>

		<a class='btn btn-outline-primary' target='_blank' :href='generateDownloadURL()' tt='download'></a>
	</div>
`
});
