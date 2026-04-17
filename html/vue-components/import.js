import { validate_meta_entry, parseAndValidateImportFile } from './import-json-validator.js';
import { mnm_api, mnm_fetch_json, mnm_notify, tt_update_interface, widar } from './store.js';

export default Vue.extend({
    props: ['original_catalog_id'],
    data: function () {
        return {
            types: [], type2count: {}, source: 'url', source_url: '', file_uuid: '',
            mode: '',
            step: 0,
            show_json_spec: false,
            json_file_name: '',
            json_data_format: 'json',
            json_preview: null,
            json_error: '',
            json_warnings: [],
            json_raw: null,
            json_raw_text: '',
            json_submitting: false,
            file_uploading: false, file_upload_pct: 0,
            importing: false,
            test_error: '',
            preview_rows: [],
            update_info: {
                url_pattern: '',
                data_format: 'csv',
                columns: [],
                num_header_rows: 1,
                min_cols: 2,
                default_type: '',
                update_existing_description: false,
                update_all_descriptions: false,
                add_new_entries: true,
                default_aux: []
            },
            top_sections_done: false,
            headers_loaded: false,
            test_running: false,
            test_successful: false,
            test_results: {},
            is_done: false,
            seconds: 0,
            meta: { catalog_id: '', name: '', desc: '', property: '', lang: 'en', type: 'unknown', url: '' },
            valid_column_headers: ['id', 'name', 'desc', 'url', 'type', 'q', 'autoq', 'born', 'died'],
            property_already_used: false,
            property_used_by: ''
        };
    },
    created: function () { this.init() },
    updated: function () { tt_update_interface() },
    mounted: function () { tt_update_interface() },
    computed: {
        steps: function () {
            if (this.mode === 'new') return ['Mode', 'Catalog', 'Data source', 'Columns', 'Import'];
            if (this.mode === 'json') return ['Mode', 'Upload JSON'];
            return ['Mode', 'Data source', 'Columns', 'Import'];
        },
        dataSourceStep: function () {
            return this.mode === 'new' ? 2 : 1;
        },
        columnStep: function () {
            return this.mode === 'new' ? 3 : 2;
        },
        reviewStep: function () {
            return this.mode === 'new' ? 4 : 3;
        },
        has_valid_source: function () {
            if (this.source == 'url' && this.source_url != '') {
                if (!this.headers_loaded) this.load_headers();
                return true;
            }
            if (this.source == 'file' && this.file_uuid != '') {
                if (!this.headers_loaded) this.load_headers();
                return true;
            }
            return false;
        },
        has_valid_source_simple: function () {
            if (this.source == 'url' && this.source_url != '') return true;
            if (this.source == 'file' && this.file_uuid != '') return true;
            return false;
        },
        has_columns: function () {
            let has_id = false;
            let has_name = false;
            this.update_info.columns.forEach(function (c) {
                if (c.use == 'id') has_id = true;
                if (c.use == 'name') has_name = true;
            });
            return has_id && has_name;
        },
        mappedColumns: function () {
            return this.update_info.columns.filter(function (c) { return c.use !== '?'; });
        },
        available_headers: function () {
            let headers = [
                { value: 'id', label: 'ID (external identifier)' },
                { value: 'name', label: 'Name' },
                { value: 'desc', label: 'Description' },
                { value: 'url', label: 'URL' },
                { value: 'type', label: 'Type (Q-ID)' },
                { value: 'q', label: 'Wikidata Q-ID' },
                { value: 'autoq', label: 'Auto Q-ID' },
                { value: 'born', label: 'Date of birth' },
                { value: 'died', label: 'Date of death' },
            ];
            // Add any P-number columns that were auto-detected
            let self = this;
            self.update_info.columns.forEach(function (c) {
                if (/^P\d+$/.test(c.use)) {
                    let exists = headers.some(function (h) { return h.value === c.use; });
                    if (!exists) headers.push({ value: c.use, label: c.use + ' (Wikidata property)' });
                }
            });
            return headers;
        }
    },
    methods: {
        init: function () {
            if (typeof this.original_catalog_id != 'undefined') {
                this.meta.catalog_id = this.original_catalog_id;
                this.mode = 'update';
                this.step = 1; // Go straight to data source for updates
            }
            this.updateTypes();
        },
        ucFirst: function (s) {
            return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, ' ');
        },
        truncate: function (s, len) {
            if (!s) return '';
            return s.length > len ? s.substring(0, len) + '\u2026' : s;
        },
        formatSeconds: function (s) {
            s = parseInt(s);
            if (s >= 31536000) return 'year';
            if (s >= 2592000) return 'month';
            if (s >= 604800) return 'week';
            return s + ' seconds';
        },
        goToStep: function (i) {
            if (i < this.step) this.step = i;
        },
        advanceFromStep0: function () {
            if (this.mode === 'new') {
                this.meta.catalog_id = '';
                this.step = 1;
            } else if (this.mode === 'update') {
                this.step = 1; // dataSourceStep for update
            } else if (this.mode === 'json') {
                this.step = 1; // dataSourceStep for json
            }
        },
        goToColumns: function () {
            this.step = this.columnStep;
            if (!this.headers_loaded) this.load_headers();
        },
        goToReview: function () {
            this.step = this.reviewStep;
            this.test_source();
        },
        isColumnUsed: function (value) {
            return this.update_info.columns.some(function (c) { return c.use === value; });
        },
        on_json_file_selected: function (event) {
            let self = this;
            self.json_error = '';
            self.json_warnings = [];
            self.json_preview = null;
            self.json_raw = null;
            self.json_raw_text = '';
            let file = event.target.files[0];
            if (!file) return;
            self.json_file_name = file.name;
            let reader = new FileReader();
            reader.onload = function (e) {
                let text = e.target.result;
                self.json_raw_text = text;
                let result = parseAndValidateImportFile(text, self.json_data_format, self.meta.catalog_id);
                self.json_warnings = result.warnings;
                if (result.catalogId && !self.meta.catalog_id) self.meta.catalog_id = result.catalogId;
                if (result.error) {
                    self.json_error = result.error;
                    return;
                }
                self.json_raw = result.entries;
                self.json_preview = result.preview;
            };
            reader.readAsText(file);
        },
        submit_json: async function () {
            let self = this;
            if (!self.json_raw || !self.meta.catalog_id) return;
            self.json_submitting = true;
            let content, mime_type, filename;
            if (self.json_data_format === 'jsonl') {
                content = self.json_raw_text;
                mime_type = 'application/x-jsonlines';
                filename = self.json_file_name || 'import.jsonl';
            } else {
                content = JSON.stringify(self.json_raw);
                mime_type = 'application/json';
                filename = self.json_file_name || 'import.json';
            }
            let blob = new Blob([content], { type: mime_type });
            let formData = new FormData();
            formData.append('query', 'upload_import_file');
            formData.append('data_format', self.json_data_format);
            formData.append('username', widar.getUserName());
            formData.append('import_file', blob, filename);
            try {
                let resp = await fetch('/api.php', { method: 'POST', body: formData });
                let data = await resp.json();
                self.json_submitting = false;
                if (data.status != 'OK') {
                    self.json_error = data.status;
                    return;
                }
                self.is_done = true;
            } catch (e) {
                self.json_submitting = false;
                self.json_error = e.message || 'Upload failed';
            }
        },
        on_file_upload: function () {
            let self = this;
            self.file_uploading = true;
            self.file_upload_pct = 0;
            let form = document.querySelector('form.file_upload_form');
            var data = new FormData(form);
            data.append('query', 'upload_import_file');
            data.append('data_format', self.update_info.data_format);
            data.append('username', widar.getUserName());
            var xhr = new XMLHttpRequest();
            xhr.upload.addEventListener('progress', function (e) {
                if (e.lengthComputable) self.file_upload_pct = Math.round(e.loaded / e.total * 100);
            });
            xhr.addEventListener('load', function () {
                self.file_uploading = false;
                try {
                    var result = JSON.parse(xhr.responseText);
                    if (result.status != 'OK') { mnm_notify(result.status, 'danger'); return; }
                    self.file_uuid = result.uuid;
                    self.source = 'file';
                } catch (e) { mnm_notify('Invalid response from server', 'danger'); }
            });
            xhr.addEventListener('error', function () {
                self.file_uploading = false;
                mnm_notify('Upload failed — network error', 'danger');
            });
            xhr.open('POST', '/api.php');
            xhr.send(data);
        },
        get_update_info: function () {
            let ret = JSON.parse(JSON.stringify(this.update_info)); // Deep clone
            if (this.source == 'url') ret.source_url = this.source_url;
            if (this.source == 'file') ret.file_uuid = this.file_uuid;
            let columns = [];
            this.update_info.columns.forEach(function (c) {
                if (c.use == '?') columns.push('');
                else columns.push(c.use);
            });
            ret.columns = columns;
            return ret;
        },
        import_source: async function () {
            let self = this;
            self.importing = true;
            try {
                let d = await mnm_api('import_source', {
                    update_info: JSON.stringify(self.get_update_info()),
                    meta: JSON.stringify(self.meta),
                    catalog: self.meta.catalog_id == '' ? 0 : self.meta.catalog_id * 1,
                    seconds: self.seconds,
                    username: widar.getUserName()
                }, { method: 'POST' });
                self.importing = false;
                self.meta.catalog_id = d.catalog_id;
                self.is_done = true;
            } catch (e) {
                self.importing = false;
                mnm_notify(e.message || 'Import failed', 'danger');
            }
        },
        test_source: async function () {
            let self = this;
            self.test_running = true;
            self.test_successful = false;
            self.test_error = '';
            let update_info = self.get_update_info();
            update_info.read_max_rows = 1000;
            try {
                let d = await mnm_api('test_import_source', {
                    update_info: JSON.stringify(update_info)
                }, { method: 'POST' });
                self.test_results = d.data;
                self.test_successful = true;
                self.test_running = false;
            } catch (e) {
                self.test_running = false;
                self.test_error = e.message || 'Request failed';
            }
        },
        load_headers: async function () {
            let self = this;
            self.headers_loaded = false;
            self.test_successful = false;
            self.preview_rows = [];
            try {
                let d = await mnm_api('get_source_headers', {
                    update_info: JSON.stringify(self.get_update_info())
                }, { method: 'POST' });
                let columns = [];
                (d.data || []).forEach(function (v) {
                    let use = '?';
                    let v2 = v.toLowerCase().trim();
                    if (self.valid_column_headers.indexOf(v2) != -1) use = v2;
                    if (v2 == 'description') use = 'desc';
                    if (v2 == 'coord') use = 'P625';
                    if (/^p\d+$/.test(v2)) use = v2.toUpperCase();
                    columns.push({ label: v, use: use });
                });
                self.update_info.columns = columns;
                self.headers_loaded = true;
                self.load_preview_rows();
            } catch (e) {
                mnm_notify(e.message || 'Failed to load headers', 'danger');
            }
        },
        load_preview_rows: async function () {
            let self = this;
            let update_info = self.get_update_info();
            update_info.read_max_rows = 10;
            try {
                let d = await mnm_api('test_import_source', {
                    update_info: JSON.stringify(update_info)
                }, { method: 'POST' });
                if (d.rows) {
                    self.preview_rows = d.rows;
                }
            } catch (e) {
                // Ignore preview errors
            }
        },
        updateTypes: async function () {
            const me = this;
            me.types = [];
            me.type2count = {};
            let d = await mnm_api('catalog_type_counts');
            (d.data || []).forEach(function (v) {
                me.type2count[v.type] = v.cnt;
                me.types.push(v.type);
            });
            me.types.sort();
        },
        doesCatalogWithPropertyExist: async function () {
            const me = this;
            var prop = (me.meta.property || '').replace(/\D/g, '');
            if (prop == '') return;
            let d = await mnm_api('check_wd_prop_usage', { wd_prop: prop });
            if (d.data && d.data.used) {
                me.property_already_used = true;
                me.property_used_by = d.data.catalog_name + ' (#' + d.data.catalog_id + ')';
            } else {
                me.property_already_used = false;
                me.property_used_by = '';
            }
        },
        onPropertyChanged: async function () {
            const me = this;
            me.doesCatalogWithPropertyExist();
            var prop = me.meta.property.replace(/\D/g, '');
            if (prop == '') return;
            prop = "P" + prop;
            let d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
                action: 'wbgetentities',
                ids: prop,
                format: 'json',
                origin: '*'
            });
            var e = d.entities[prop];
            if (typeof e == 'undefined') return;
            if (me.meta.name == '' && typeof e.labels.en != 'undefined') me.meta.name = e.labels.en.value.replace(/ ID$/, '');
            if (me.meta.desc == '' && typeof e.descriptions.en != 'undefined') me.meta.desc = e.descriptions.en.value;
            if (typeof e.claims == 'undefined') return;
            if (me.meta.url == '' && typeof e.claims['P1896'] != 'undefined') me.meta.url = e.claims['P1896'][0].mainsnak.datavalue.value;
            if (me.update_info.url_pattern == '' && typeof e.claims['P1630'] != 'undefined') me.update_info.url_pattern = e.claims['P1630'][0].mainsnak.datavalue.value;
        },
    },
    template: `
<div class='mt-2'>
	<mnm-breadcrumb :crumbs="[{tt: 'import_or_update_catalog'}]"></mnm-breadcrumb>

	<!-- Header -->
	<div class="d-flex justify-content-between align-items-center mb-3">
		<h1 class="mb-0" tt='import_or_update_catalog'></h1>
		<a href="https://meta.wikimedia.org/wiki/Mix'n'match/Import" target='_blank'
			class='btn btn-outline-secondary btn-sm'>
			&#x2139; Help
		</a>
	</div>

	<!-- Step indicator -->
	<div class="mb-4" v-if="!is_done">
		<div class="d-flex justify-content-between position-relative" style="z-index:1">
			<div v-for="(s,i) in steps" :key="i"
				class="text-center flex-fill"
				:style="{ cursor: i < step ? 'pointer' : 'default' }"
				@click="goToStep(i)">
				<div class="rounded-circle d-inline-flex align-items-center justify-content-center mb-1"
					:style="{
						width: '36px', height: '36px',
						background: i < step ? '#00af89' : i === step ? '#36c' : '#c8ccd1',
						color: '#fff', fontWeight: 'bold', fontSize: '14px',
						transition: 'background 0.3s'
					}">
					<span v-if="i < step">&#x2713;</span>
					<span v-else>{{i + 1}}</span>
				</div>
				<div :style="{ fontSize: '12px', color: i <= step ? '#202122' : '#72777d', fontWeight: i === step ? 'bold' : 'normal' }">
					{{s}}
				</div>
			</div>
		</div>
		<div class="mt-1" style="height:4px; background:#eaecf0; border-radius:2px">
			<div :style="{ width: (step / (steps.length - 1) * 100) + '%', height: '100%', background: '#36c', borderRadius: '2px', transition: 'width 0.4s ease' }"></div>
		</div>
	</div>

	<!-- ===================== STEP 0: Choose mode ===================== -->
	<div v-if="step === 0 && !is_done">
		<div class="card border-0 shadow-sm mb-3">
			<div class="card-body">
				<h5 class="card-title mb-3">What would you like to do?</h5>

				<div class="row">
					<div class="col-sm-4 mb-3">
						<div class="card h-100 border-2 text-center p-3"
							:class="{ 'border-primary': mode === 'new' }"
							style="cursor:pointer; transition: all 0.2s"
							@click="mode='new'; meta.catalog_id=''">
							<div style="font-size:2.5rem; line-height:1">&#x2795;</div>
							<h6 class="mt-2 mb-1">New catalog</h6>
							<small class="text-muted">Create a brand new catalog from your data</small>
						</div>
					</div>
					<div class="col-sm-4 mb-3">
						<div class="card h-100 border-2 text-center p-3"
							:class="{ 'border-primary': mode === 'update' }"
							style="cursor:pointer; transition: all 0.2s"
							@click="mode='update'">
							<div style="font-size:2.5rem; line-height:1">&#x1F504;</div>
							<h6 class="mt-2 mb-1">Update existing</h6>
							<small class="text-muted">Add or refresh entries in an existing catalog</small>
						</div>
					</div>
					<div class="col-sm-4 mb-3">
						<div class="card h-100 border-2 text-center p-3"
							:class="{ 'border-primary': mode === 'json' }"
							style="cursor:pointer; transition: all 0.2s"
							@click="mode='json'">
							<div style="font-size:2.5rem; line-height:1">{ }</div>
							<h6 class="mt-2 mb-1">JSON import</h6>
							<small class="text-muted">Upload a JSON file (processed by job queue)</small>
						</div>
					</div>
				</div>

				<!-- Update: catalog ID -->
				<div v-if="mode === 'update'" class="mt-3">
					<div class="mb-3 row align-items-center">
						<label class="col-sm-3 col-form-label fw-bold" tt='catalog_id'></label>
						<div class="col-sm-3">
							<input type="number" class="form-control" v-model="meta.catalog_id" placeholder="e.g. 1234">
						</div>
					</div>
				</div>

				<!-- JSON mode info -->
				<div v-if="mode === 'json'" class="mt-3">
					<div class="alert alert-info mb-0">
						<strong>JSON / JSONL format</strong> &mdash; Upload a JSON or JSONL file. The import will be queued for processing by the backend job system.
						<button class="btn btn-sm btn-outline-info mt-2" @click.prevent="show_json_spec = !show_json_spec">
							{{show_json_spec ? 'Hide' : 'Show'}} format specification
						</button>
						<div v-if="show_json_spec" class="mt-2">
							<div class="mb-1"><strong>JSON</strong> &mdash; an array of MetaEntry objects:</div>
							<pre class="p-3 bg-white border rounded mb-2" style="font-size:12px; max-height:220px; overflow:auto">[
  {
    "entry": {
      "catalog": 1234,
      "ext_id": "n2014191777",
      "ext_name": "Jane Doe",
      "ext_desc": "American physicist, 1920-2005",
      "ext_url": "https://example.org/record/n2014191777",
      "type_name": "Q5"
    },
    "auxiliary": [
      { "prop_numeric": 214, "value": "113084680" }
    ],
    "person_dates": { "born": "1920-03-15", "died": "2005" },
    "coordinate": { "lat": 51.5074, "lon": -0.1278 },
    "descriptions": { "en": "American physicist" },
    "aliases": [ { "language": "en", "value": "J. Doe" } ]
  }
]</pre>
							<div class="mb-1"><strong>JSONL</strong> &mdash; one MetaEntry object per line:</div>
							<pre class="p-3 bg-white border rounded mb-2" style="font-size:12px; max-height:120px; overflow:auto">{"entry":{"catalog":1234,"ext_id":"abc","ext_name":"Jane Doe","type_name":"Q5"}}
{"entry":{"catalog":1234,"ext_id":"def","ext_name":"Kew Gardens","type_name":"Q167346"},"coordinate":{"lat":51.47,"lon":-0.29}}</pre>
							<div class="mb-1"><small class="text-muted">
								Only <code>entry.catalog</code>, <code>entry.ext_id</code>, and <code>entry.ext_name</code> are required.
								Optional fields: <code>ext_url</code>, <code>ext_desc</code>, <code>type_name</code>, <code>q</code>, <code>user</code>.
								Optional sections: <code>auxiliary</code>, <code>coordinate</code>, <code>person_dates</code>, <code>descriptions</code>, <code>aliases</code>, <code>mnm_relations</code>, <code>kv_entries</code>.
							</small></div>
						</div>
					</div>
				</div>

				<div class="mt-3 text-end" v-if="mode !== ''">
					<button class="btn btn-primary" @click.prevent="advanceFromStep0"
						:disabled="mode === 'update' && meta.catalog_id === ''">
						Continue &#x2192;
					</button>
				</div>
			</div>
		</div>
	</div>

	<!-- ===================== STEP 1: Catalog details (new only) ===================== -->
	<div v-if="step === 1 && !is_done && mode === 'new'">
		<div class="card border-0 shadow-sm mb-3">
			<div class="card-body">
				<h5 class="card-title mb-3">Catalog details</h5>

				<div class="mb-3 row">
					<label class="col-sm-3 col-form-label" tt='catalog_name'></label>
					<div class="col-sm-9">
						<input type="text" class="form-control" v-model="meta.name" placeholder="Short name, e.g. VIAF">
					</div>
				</div>
				<div class="mb-3 row">
					<label class="col-sm-3 col-form-label" tt='description'></label>
					<div class="col-sm-9">
						<input type="text" class="form-control" v-model="meta.desc" maxlength="250" placeholder="Brief description of the catalog">
					</div>
				</div>
				<div class="mb-3 row">
					<label class="col-sm-3 col-form-label" tt='url'></label>
					<div class="col-sm-9">
						<input type="text" class="form-control" v-model="meta.url" placeholder="https://example.org (optional)">
					</div>
				</div>
				<div class="mb-3 row align-items-center">
					<label class="col-sm-3 col-form-label" tt='wd_property'></label>
					<div class="col-sm-3">
						<input type="text" class="form-control" v-model="meta.property" @blur="onPropertyChanged" placeholder="P1234">
					</div>
					<div class="col-sm-6">
						<span v-if="property_already_used" class="text-warning">
							&#x26A0; Property already used by {{property_used_by}}
						</span>
					</div>
				</div>
				<div class="mb-3 row">
					<label class="col-sm-3 col-form-label" tt='type'></label>
					<div class="col-sm-3">
						<select class="form-select" v-model='meta.type'>
							<option v-for='group in types' :value='group'>{{ucFirst(group)}}</option>
						</select>
					</div>
					<label class="col-sm-2 col-form-label" tt='primary_language'></label>
					<div class="col-sm-2">
						<input type="text" class="form-control" v-model="meta.lang">
					</div>
				</div>
				<div class="mb-3 row">
					<label class="col-sm-3 col-form-label" tt='default_entry_type'></label>
					<div class="col-sm-3">
						<input type='text' class="form-control" v-model='update_info.default_type' placeholder='e.g. Q5'>
					</div>
					<div class="col-sm-6">
						<small class="text-muted">
							If set to a Q-ID (e.g. Q11424), automatches will be limited to items
							of that type and its subclasses.
						</small>
					</div>
				</div>

				<div class="d-flex justify-content-between mt-3">
					<button class="btn btn-outline-secondary" @click.prevent="step=0">&larr; Back</button>
					<button class="btn btn-primary" @click.prevent="step=2"
						:disabled="meta.name === '' || meta.desc === ''">
						Continue &#x2192;
					</button>
				</div>
			</div>
		</div>
	</div>

	<!-- ===================== STEP 1/2: Data source ===================== -->
	<div v-if="step === dataSourceStep && !is_done && mode !== 'json'">
		<div class="card border-0 shadow-sm mb-3">
			<div class="card-body">
				<h5 class="card-title mb-3">Data source</h5>

				<!-- Format -->
				<div class="mb-3 row align-items-center">
					<label class="col-sm-3 col-form-label fw-bold" tt='file_type'></label>
					<div class="col-sm-9">
						<div class="btn-group" role="group">
							<button class="btn" :class="update_info.data_format === 'csv' ? 'btn-primary' : 'btn-outline-secondary'"
								@click.prevent="update_info.data_format='csv'">CSV (comma)</button>
							<button class="btn" :class="update_info.data_format === 'tsv' ? 'btn-primary' : 'btn-outline-secondary'"
								@click.prevent="update_info.data_format='tsv'">TSV (tab)</button>
							<button class="btn" :class="update_info.data_format === 'ssv' ? 'btn-primary' : 'btn-outline-secondary'"
								@click.prevent="update_info.data_format='ssv'">SSV (semicolon)</button>
						</div>
					</div>
				</div>

				<!-- Source selection -->
				<div class="mb-3 row">
					<label class="col-sm-3 col-form-label fw-bold" tt='source'></label>
					<div class="col-sm-9">
						<!-- URL source -->
						<div class="card mb-2 p-3" :class="{ 'border-primary': source === 'url' }"
							style="cursor:pointer" @click="source='url'">
							<div class="d-flex align-items-center mb-2">
								<input type="radio" v-model="source" value='url' class="me-2">
								<strong>From URL</strong>
							</div>
							<div v-if="source === 'url'">
								<input type="text" class="form-control mb-2" v-model="source_url"
									placeholder="https://example.org/data.csv" @blur="onPropertyChanged">
								<div class="d-flex flex-wrap">
									<label class="me-3 mb-1">
										<input type='radio' v-model='seconds' value='0' class="me-1">
										<span tt='no_auto_update'></span>
									</label>
									<label class="me-3 mb-1">
										<input type='radio' v-model='seconds' value='604800' class="me-1">
										<span tt='auto_update_week'></span>
									</label>
									<label class="me-3 mb-1">
										<input type='radio' v-model='seconds' value='2592000' class="me-1">
										<span tt='auto_update_month'></span>
									</label>
									<label class="mb-1">
										<input type='radio' v-model='seconds' value='31536000' class="me-1">
										<span tt='auto_update_year'></span>
									</label>
								</div>
							</div>
						</div>

						<!-- File upload source -->
						<div class="card p-3" :class="{ 'border-primary': source === 'file' }"
							style="cursor:pointer" @click="source='file'">
							<div class="d-flex align-items-center mb-2">
								<input type="radio" v-model="source" value='file' class="me-2">
								<strong>Upload file</strong>
							</div>
							<div v-if="source === 'file'">
								<div v-if='widar.is_logged_in'>
									<form class='file_upload_form'>
										<div style="max-width:400px">
											<input type="file" name="import_file" class="form-control"
												id="importFileInput" @change="on_file_upload">
										</div>
									</form>
									<div v-if="file_uuid" class="mt-2">
										<span class="badge text-bg-success">&#x2713; Uploaded</span>
										<small class="text-muted ms-1">{{file_uuid}}</small>
									</div>
									<div v-if="file_uploading" class="mt-2">
										<div class="progress" style="height:6px;max-width:300px">
											<div class="progress-bar" role="progressbar" :style="'width:'+file_upload_pct+'%'" :aria-valuenow="file_upload_pct" aria-valuemin="0" aria-valuemax="100"></div>
										</div>
										<small class="text-muted">Uploading&hellip; {{file_upload_pct}}%</small>
									</div>
								</div>
								<div v-else class="text-danger" tt='log_into_widar'></div>
							</div>
						</div>
					</div>
				</div>

				<!-- Update modes -->
				<div class="mb-3 row" v-if="mode === 'update'">
					<label class="col-sm-3 col-form-label fw-bold" tt='update_modes'></label>
					<div class="col-sm-9">
						<div class="form-check">
							<input type='checkbox' class="form-check-input" id="chkAddNew" v-model='update_info.add_new_entries'>
							<label class="form-check-label" for="chkAddNew" tt='add_new_entries'></label>
						</div>
						<div class="form-check">
							<input type='checkbox' class="form-check-input" id="chkUpdateDesc" v-model='update_info.update_existing_description'>
							<label class="form-check-label" for="chkUpdateDesc" tt='update_existing_description'></label>
						</div>
						<div class="form-check">
							<input type='checkbox' class="form-check-input" id="chkUpdateAllDesc" v-model='update_info.update_all_descriptions'>
							<label class="form-check-label" for="chkUpdateAllDesc" tt='update_all_descriptions'></label>
						</div>
					</div>
				</div>

				<div class="d-flex justify-content-between mt-3">
					<button class="btn btn-outline-secondary" @click.prevent="step = mode === 'new' ? 1 : 0">&larr; Back</button>
					<button class="btn btn-primary" @click.prevent="goToColumns"
						:disabled="!has_valid_source_simple">
						Load columns &#x2192;
					</button>
				</div>
			</div>
		</div>
	</div>

	<!-- ===================== STEP 1/2: JSON upload ===================== -->
	<div v-if="step === dataSourceStep && !is_done && mode === 'json'">
		<div class="card border-0 shadow-sm mb-3">
			<div class="card-body">
				<h5 class="card-title mb-3">Upload JSON file</h5>

				<div class="mb-3 row align-items-center">
					<label class="col-sm-3 col-form-label fw-bold">Format</label>
					<div class="col-sm-9">
						<div class="btn-group" role="group">
							<button class="btn" :class="json_data_format === 'json' ? 'btn-primary' : 'btn-outline-secondary'"
								@click.prevent="json_data_format='json'">JSON</button>
							<button class="btn" :class="json_data_format === 'jsonl' ? 'btn-primary' : 'btn-outline-secondary'"
								@click.prevent="json_data_format='jsonl'">JSONL (one object per line)</button>
						</div>
					</div>
				</div>

				<div class="mb-3 row align-items-center">
					<label class="col-sm-3 col-form-label fw-bold">Catalog ID</label>
					<div class="col-sm-3">
						<input type="number" class="form-control" v-model="meta.catalog_id" placeholder="e.g. 1234">
					</div>
					<div class="col-sm-6">
						<small class="text-muted">Must match the <code>entry.catalog</code> field in each entry</small>
					</div>
				</div>

				<div v-if='widar.is_logged_in'>
					<div class="mb-3">
						<label class="fw-bold">{{json_data_format.toUpperCase()}} file</label>
						<div style="max-width:500px">
							<input type="file" :accept="json_data_format === 'json' ? '.json' : '.jsonl,.json'"
								class="form-control"
								id="jsonFileInput" @change="on_json_file_selected">
						</div>
					</div>

					<!-- JSON preview -->
					<div v-if="json_preview" class="mt-3">
						<h6>Preview</h6>
						<div class="border rounded p-3 bg-light" style="max-height:300px; overflow:auto">
							<div class="mb-2">
								<span class="badge text-bg-secondary">Catalog: {{json_preview.catalog}}</span>
								<span class="badge text-bg-secondary">Entries: {{json_preview.entry_count}}</span>
								<span class="badge text-bg-success" v-if="json_preview.valid">&#x2713; Valid</span>
							</div>
							<table class="table table-sm table-bordered mb-0" style="font-size:12px" v-if="json_preview.sample.length">
								<thead class="table-light">
									<tr>
										<th>ext_id</th>
										<th>ext_name</th>
										<th>ext_desc</th>
										<th>type_name</th>
										<th>Extras</th>
									</tr>
								</thead>
								<tbody>
									<tr v-for="(row,ri) in json_preview.sample" :key="ri">
										<td><code>{{row.entry.ext_id}}</code></td>
										<td>{{row.entry.ext_name}}</td>
										<td class="text-muted">{{truncate(row.entry.ext_desc || '', 50)}}</td>
										<td><code>{{row.entry.type_name || ''}}</code></td>
										<td>
											<span v-if="row.auxiliary && row.auxiliary.length" class="badge text-bg-light me-1" :title="row.auxiliary.length + ' auxiliary properties'">aux:{{row.auxiliary.length}}</span>
											<span v-if="row.coordinate" class="badge text-bg-light me-1" title="Has coordinates">coord</span>
											<span v-if="row.person_dates" class="badge text-bg-light me-1" title="Has person dates">dates</span>
											<span v-if="row.descriptions && Object.keys(row.descriptions).length" class="badge text-bg-light me-1" :title="Object.keys(row.descriptions).length + ' descriptions'">desc:{{Object.keys(row.descriptions).length}}</span>
											<span v-if="row.aliases && row.aliases.length" class="badge text-bg-light me-1" :title="row.aliases.length + ' aliases'">alias:{{row.aliases.length}}</span>
										</td>
									</tr>
								</tbody>
							</table>
							<div v-if="json_preview.entry_count > 5" class="text-muted mt-1" style="font-size:11px">
								Showing 5 of {{json_preview.entry_count}} entries
							</div>
						</div>
					</div>

					<!-- Validation warnings -->
					<div v-if="json_warnings.length" class="alert alert-warning mt-3">
						<strong>Warnings ({{json_warnings.length}}):</strong>
						<ul class="mb-0 mt-1 ps-3" style="max-height:150px; overflow:auto">
							<li v-for="(w,i) in json_warnings" :key="i" style="font-size:13px">{{w}}</li>
						</ul>
					</div>

					<!-- Validation errors -->
					<div v-if="json_error" class="alert alert-danger mt-3">
						<div v-if="typeof json_error === 'string'">{{json_error}}</div>
						<div v-else>
							<strong>Validation errors ({{json_error.length}}):</strong>
							<ul class="mb-0 mt-1 ps-3" style="max-height:200px; overflow:auto">
								<li v-for="(e,i) in json_error" :key="i" style="font-size:13px">{{e}}</li>
							</ul>
						</div>
					</div>
				</div>
				<div v-else class="alert alert-warning" tt='log_into_widar'></div>

				<div class="d-flex justify-content-between mt-3">
					<button class="btn btn-outline-secondary" @click.prevent="step=0">&larr; Back</button>
					<button class="btn btn-success" @click.prevent="submit_json"
						:disabled="!json_preview || !json_preview.valid || !meta.catalog_id || json_submitting">
						<span v-if="json_submitting">
							<span class="spinner-border spinner-border-sm" role="status"></span> Submitting&hellip;
						</span>
						<span v-else>Queue JSON import</span>
					</button>
				</div>
			</div>
		</div>
	</div>

	<!-- ===================== STEP 3: Column mapping ===================== -->
	<div v-if="step === columnStep && !is_done">
		<div class="card border-0 shadow-sm mb-3">
			<div class="card-body">
				<div class="d-flex justify-content-between align-items-center mb-3">
					<h5 class="card-title mb-0">Column mapping</h5>
					<button class='btn btn-outline-secondary btn-sm' @click.prevent='load_headers'>
						&#x21BB; Reload
					</button>
				</div>

				<div v-if="!headers_loaded" class="text-center py-4">
					<div class="spinner-border text-primary" role="status"></div>
					<div class="mt-2 text-muted" tt='getting_headers'></div>
				</div>

				<div v-else>
					<!-- Validation alerts -->
					<div v-if="!has_columns" class="alert alert-warning py-2">
						&#x26A0; You need at least an <strong>id</strong> and a <strong>name</strong> column mapped.
					</div>

					<div class="table-responsive"><table class='table table-hover mb-3'>
						<thead class="table-light">
							<tr>
								<th style="width:50px">#</th>
								<th>Column header</th>
								<th style="width:200px">Map to</th>
								<th>Sample values</th>
							</tr>
						</thead>
						<tbody>
							<tr v-for='(c,num) in update_info.columns' :key="num"
								:class="{ 'table-success': c.use !== '?' }">
								<td class="text-muted">{{num + 1}}</td>
								<td><strong>{{c.label}}</strong></td>
								<td>
									<select class="form-select form-select-sm" v-model="c.use">
										<option value="?">-- skip --</option>
										<option v-for="h in available_headers" :value="h.value"
											:disabled="h.value !== '?' && h.value !== c.use && isColumnUsed(h.value)">
											{{h.label}}
										</option>
									</select>
								</td>
								<td>
									<small class="text-muted" v-if="preview_rows.length">
										<span v-for="(row,ri) in preview_rows.slice(0,3)" :key="ri">
											<code>{{truncate(row[num] || '', 30)}}</code>
											<span v-if="ri < 2 && ri < preview_rows.length - 1" class="mx-1">|</span>
										</span>
									</small>
								</td>
							</tr>
						</tbody>
					</table></div>

					<!-- Data preview table -->
					<div v-if="preview_rows.length" class="mb-3">
						<h6>Data preview <small class="text-muted">(first {{preview_rows.length}} rows)</small></h6>
						<div style="overflow-x:auto; max-height:300px">
							<table class="table table-sm table-bordered table-striped" style="font-size:12px">
								<thead class="table-dark">
									<tr>
										<th v-for="(c,num) in update_info.columns" :key="num"
											:class="{ 'text-success': c.use !== '?' }">
											{{c.use !== '?' ? c.use : c.label}}
										</th>
									</tr>
								</thead>
								<tbody>
									<tr v-for="(row,ri) in preview_rows" :key="ri">
										<td v-for="(cell,ci) in row" :key="ci"
											:class="{ 'fw-bold': update_info.columns[ci] && update_info.columns[ci].use === 'name' }">
											{{truncate(cell || '', 50)}}
										</td>
									</tr>
								</tbody>
							</table>
						</div>
					</div>

					<div class="d-flex justify-content-between">
						<button class="btn btn-outline-secondary" @click.prevent="step = dataSourceStep">&larr; Back</button>
						<button class='btn btn-primary' :disabled='!has_columns' @click.prevent='goToReview'>
							Review &#x2192;
						</button>
					</div>
				</div>
			</div>
		</div>
	</div>

	<!-- ===================== STEP 4: Review & Import ===================== -->
	<div v-if="step === reviewStep && !is_done">
		<div class="card border-0 shadow-sm mb-3">
			<div class="card-body">
				<h5 class="card-title mb-3">Review &amp; import</h5>

				<!-- Test results -->
				<div v-if="test_running" class="text-center py-4">
					<div class="spinner-border text-primary" role="status"></div>
					<div class="mt-2">Running validation on up to 1,000 rows&hellip;</div>
				</div>

				<div v-else-if="test_successful">
					<!-- Summary cards -->
					<div class="row mb-3">
						<div class="col-sm-6 col-md-3 mb-2" v-for="(num,label) in test_results" :key="label">
							<div class="card text-center p-2 border"
								:class="{ 'border-danger': label.toLowerCase().includes('error') && num > 0 }">
								<div style="font-size:1.5rem; font-weight:bold; font-family:monospace"
									:class="{ 'text-danger': label.toLowerCase().includes('error') && num > 0 }">
									{{num}}
								</div>
								<small class="text-muted">{{label}}</small>
							</div>
						</div>
					</div>

					<!-- Import summary -->
					<div class="alert alert-light border mb-3">
						<h6 class="mb-2">Import summary</h6>
						<ul class="mb-0 ps-3">
							<li v-if="mode === 'new'"><strong>New catalog:</strong> {{meta.name}}</li>
							<li v-else><strong>Update catalog:</strong> #{{meta.catalog_id}}</li>
							<li><strong>Source:</strong> {{source === 'url' ? source_url : 'Uploaded file'}}</li>
							<li><strong>Format:</strong> {{update_info.data_format.toUpperCase()}}</li>
							<li><strong>Mapped columns:</strong>
								<span v-for="(c,i) in mappedColumns" :key="i">
									{{c.use}}<span v-if="i < mappedColumns.length - 1">, </span>
								</span>
							</li>
							<li v-if="seconds > 0"><strong>Auto-update:</strong> every {{formatSeconds(seconds)}}</li>
						</ul>
					</div>

					<!-- Import button -->
					<div class="text-center">
						<div v-if='widar.is_logged_in'>
							<button class='btn btn-success btn-lg px-5' style='color:#fff' @click.prevent='import_source'
								:disabled="importing">
								<span v-if="importing">
									<span class="spinner-border spinner-border-sm" role="status"></span>
									Importing&hellip;
								</span>
								<span v-else tt='looks_good_import'></span>
							</button>
						</div>
						<div v-else class="alert alert-warning" tt='log_into_widar'></div>
					</div>
				</div>

				<div v-else-if="test_error" class="alert alert-danger">
					<strong>Test failed:</strong> {{test_error}}
					<button class="btn btn-sm btn-outline-danger ms-2" @click.prevent="test_source">Retry</button>
				</div>

				<div class="d-flex justify-content-between mt-3" v-if="!test_running">
					<button class="btn btn-outline-secondary" @click.prevent="step = columnStep">&larr; Back</button>
					<button class="btn btn-outline-secondary" @click.prevent="test_source" v-if="!test_successful">
						&#x21BB; Run test
					</button>
				</div>
			</div>
		</div>
	</div>

	<!-- ===================== DONE ===================== -->
	<div v-if="is_done">
		<div class="card border-0 shadow-sm">
			<div class="card-body text-center py-5">
				<div style="font-size:4rem; line-height:1; color:#00af89">&#x2713;</div>
				<h3 class="mt-3 mb-2" tt='all_done'></h3>
				<p class="text-muted mb-4">
					Your catalog has been {{mode === 'json' ? 'queued for processing' : 'imported successfully'}}.
				</p>
				<router-link :to="'/catalog/'+meta.catalog_id" class="btn btn-primary btn-lg">
					Go to catalog #{{meta.catalog_id}}
				</router-link>
			</div>
		</div>
	</div>

</div>
`
});
