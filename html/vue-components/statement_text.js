(function () {
    const s = document.createElement('style');
    s.textContent = `
	.st-group-card {
		border: 1px solid #dee2e6;
		border-radius: 6px;
		margin-bottom: 1rem;
	}

	.st-group-card .card-header {
		background: #f8f9fa;
		padding: 0.6rem 1rem;
		border-bottom: 1px solid #dee2e6;
		border-radius: 6px 6px 0 0;
		display: flex;
		align-items: center;
		justify-content: space-between;
	}

	.st-text-value {
		font-size: 1.25em;
		font-weight: 600;
		word-break: break-word;
	}

	.st-search-result {
		display: flex;
		align-items: flex-start;
		gap: 0.5rem;
		padding: 0.4rem 0.5rem;
		border: 1px solid #e9ecef;
		border-radius: 4px;
		margin-bottom: 0.35rem;
		cursor: default;
	}

	.st-search-result:hover {
		background: #f8f9fa;
	}

	.st-result-meta {
		flex: 1;
		min-width: 0;
	}

	.st-result-label {
		font-weight: 500;
		font-size: 0.95em;
	}

	.st-result-desc {
		font-size: 0.82em;
		color: #6c757d;
		white-space: nowrap;
		overflow: hidden;
		text-overflow: ellipsis;
	}

	.st-sample-list {
		font-size: 0.88em;
		list-style: none;
		padding: 0;
		margin: 0.25rem 0 0;
	}

	.st-sample-list li {
		padding: 0.1rem 0;
		color: #495057;
	}

	.st-section-label {
		font-size: 0.8em;
		text-transform: uppercase;
		letter-spacing: 0.04em;
		color: #6c757d;
		margin-bottom: 0.3rem;
	}

	.st-done-banner {
		text-align: center;
		padding: 3rem 1rem;
		color: #495057;
	}

	.st-done-banner .st-done-icon {
		font-size: 3rem;
		margin-bottom: 0.5rem;
	}
`;
    document.head.appendChild(s);
})();

import { mnm_api, mnm_fetch_json, ensure_catalog, get_specific_catalog, tt_update_interface, widar, tt } from './store.js';

export default Vue.extend({
    props: ['id'],

    data: function () {
        return {
            catalog: {},
            loading: true,
            groups: [],       // [{property, text, cnt, samples:[]}]
            properties: [],   // [{property, group_count}] for the dropdown
            prop_labels: {},  // {property_num: 'English label'}
            selected_property: 0, // 0 = all properties
            current_index: 0,
            done_count: 0,
            search_query: '',
            search_results: [],
            searching: false,
            searched: false,
            manual_q: '',
            saving: false,
            error_msg: ''
        };
    },

    computed: {
        current: function () {
            if (!this.groups.length) return null;
            return this.groups[this.current_index] || null;
        },
        progress_pct: function () {
            let total = this.done_count + this.groups.length;
            if (!total) return 0;
            return Math.round(100 * this.done_count / total);
        }
    },

    created: async function () {
        await ensure_catalog(this.id);
        this.catalog = get_specific_catalog(this.id);
        this.load_groups();
    },

    updated: function () { tt_update_interface(); },
    mounted: function () { tt_update_interface(); },

    methods: {

        load_groups: async function () {
            let me = this;
            me.loading = true;
            me.current_index = 0;
            let params = {
                catalog: me.id,
                limit: 50,
                offset: 0
            };
            if (me.selected_property > 0) params.property = me.selected_property;
            try {
                let d = await mnm_api('get_statement_text_groups', params);
                me.groups = (d.data && d.data.groups) ? d.data.groups : [];
                if (d.data && d.data.properties) {
                    me.properties = d.data.properties.map(function (p) {
                        return {
                            property: parseInt(p.property, 10),
                            group_count: parseInt(p.group_count, 10)
                        };
                    });
                    me.fetch_prop_labels();
                }
                me.loading = false;
                if (me.current) me.on_group_change();
            } catch (e) {
                me.loading = false;
            }
        },

        reload: function () {
            this.done_count = 0;
            this.selected_property = 0;
            this.properties = [];
            this.load_groups();
        },

        on_property_change: function () {
            this.done_count = 0;
            this.groups = [];
            this.load_groups();
        },

        // Fetch English labels from Wikidata for any properties not yet in prop_labels.
        fetch_prop_labels: async function () {
            let me = this;
            if (!me.properties.length) return;
            let to_fetch = me.properties
                .map(function (p) { return p.property; })
                .filter(function (num) { return !me.prop_labels[num]; });
            if (!to_fetch.length) return;
            let ids = to_fetch.map(function (num) { return 'P' + num; }).join('|');
            let d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
                action: 'wbgetentities',
                ids: ids,
                props: 'labels',
                languages: 'en',
                format: 'json',
                origin: '*'
            });
            if (!d.entities) return;
            let labels = Object.assign({}, me.prop_labels);
            Object.entries(d.entities).forEach(function ([pid, entity]) {
                let num = parseInt(pid.replace(/\D/g, ''), 10);
                if (entity.labels && entity.labels.en) {
                    labels[num] = entity.labels.en.value;
                }
            });
            me.prop_labels = labels;
        },

        // Returns the display string for a property number in the dropdown.
        prop_label: function (property_num) {
            let label = this.prop_labels[property_num];
            return label ? label + ' (P' + property_num + ')' : 'P' + property_num;
        },

        on_group_change: function () {
            this.search_results = [];
            this.searched = false;
            this.manual_q = '';
            this.error_msg = '';
            if (this.current) {
                this.search_query = this.current.text;
                this.search_wikidata();
            }
        },

        skip: function () {
            let me = this;
            if (me.current_index < me.groups.length - 1) {
                me.current_index++;
            } else {
                me.current_index = 0;
            }
            me.on_group_change();
        },

        search_wikidata: async function () {
            let me = this;
            let q = me.search_query.trim();
            if (!q) return;
            me.searching = true;
            me.searched = false;
            me.search_results = [];
            try {
                let d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
                    action: 'wbsearchentities',
                    search: q,
                    language: 'en',
                    limit: 10,
                    type: 'item',
                    format: 'json',
                    origin: '*'
                });
                me.search_results = (d.search || []).map(function (r) {
                    return {
                        id: r.id,
                        label: r.label || r.id,
                        description: r.description || ''
                    };
                });
                me.searched = true;
            } finally {
                me.searching = false;
            }
        },

        set_q: function (q_str) {
            let q = parseInt(('' + q_str).replace(/\D/g, ''), 10);
            if (!q || q <= 0) {
                this.error_msg = tt.t('st_invalid_qid') + ' ' + q_str;
                return;
            }
            this.save_q(q);
        },

        set_manual_q: function () {
            let raw = this.manual_q.trim();
            if (!raw) return;
            let q = parseInt(raw.replace(/\D/g, ''), 10);
            if (!q || q <= 0) {
                this.error_msg = tt.t('st_valid_qid_hint');
                return;
            }
            this.save_q(q);
        },

        save_q: async function (q) {
            let me = this;
            if (!me.current) return;
            me.saving = true;
            me.error_msg = '';

            let property = me.current.property;
            let text = me.current.text;

            try {
                await mnm_api('set_statement_text_q', {
                    tusc_user: widar.getUserName(),
                    catalog: me.id,
                    property: property,
                    text: text,
                    q: q
                }, { method: 'POST' });
                me.done_count++;
                me.groups.splice(me.current_index, 1);
                if (me.groups.length === 0) {
                    me.saving = false;
                    return;
                }
                if (me.current_index >= me.groups.length) {
                    me.current_index = 0;
                }
                me.saving = false;
                me.on_group_change();
            } catch (e) {
                me.error_msg = e.message || tt.t('st_network_error');
                me.saving = false;
            }
        }

    },

    template: `
	<div class='mt-2'>
		<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
			{text: catalog.name, to: '/catalog/'+catalog.id},
			{text: 'Statement text'}
		]"></mnm-breadcrumb>

		<catalog-header :catalog='catalog'></catalog-header>

		<!-- Property filter — only shown when multiple properties exist -->
		<div v-if='properties.length > 0' class='mb-3 mt-2'>
			<select class='form-control form-control-sm' style='max-width:340px' v-model='selected_property'
				@change='on_property_change'>
				<option :value='0'>All properties</option>
				<option v-for='p in properties' :key='p.property' :value='p.property'>
					{{prop_label(p.property)}} ({{p.group_count}})
				</option>
			</select>
		</div>

		<!-- Loading -->
		<div v-if='loading' class='mt-3'>
			<i tt='loading'></i>
		</div>

		<!-- All done -->
		<div v-else-if='groups.length === 0' class='st-done-banner'>
			<div class='st-done-icon'>✓</div>
			<p class='mb-1'><strong tt='done'></strong></p>
			<p class='text-muted' tt='st_all_done_blurb'></p>
			<button class='btn btn-outline-secondary btn-sm mt-1' @click.prevent='reload' tt='st_check_again'></button>
		</div>

		<!-- Main UI -->
		<div v-else>

			<!-- Progress bar and controls -->
			<div class='d-flex align-items-center mb-2' style='gap:0.75rem'>
				<div class='flex-grow-1'>
					<div class='progress' style='height:6px'>
						<div class='progress-bar bg-success' role='progressbar' :style='"width:" + progress_pct + "%"'>
						</div>
					</div>
				</div>
				<small class='text-muted' style='white-space:nowrap'>
					{{done_count}} <span tt='st_matched'></span> &middot;
					{{groups.length}} <span tt='st_remaining'></span>
				</small>
				<button class='btn btn-outline-secondary btn-sm' @click.prevent='reload'
					tt_title='reload'>&#8635;</button>
			</div>

			<!-- Current group card -->
			<div class='st-group-card' v-if='current'>
				<div class='card-header'>
					<div>
						<wd-link :item='"P" + current.property' :key='"P" + current.property' smallq=1></wd-link>
						&nbsp;&middot;&nbsp;
						<strong>{{current.cnt}}</strong>
						<span style='font-size:0.9em; color:#6c757d'>
							<span v-if='current.cnt === 1' tt='st_unmatched_entry'></span>
							<span v-else tt='st_unmatched_entries'></span>
						</span>
					</div>
					<button class='btn btn-outline-secondary btn-sm' @click.prevent='skip' :disabled='saving'
						tt='skip'></button>
				</div>

				<div style='padding:1rem'>

					<!-- Text value -->
					<div class='mb-3'>
						<div class='st-section-label' tt='st_text_value'></div>
						<div class='st-text-value'>{{current.text}}</div>
					</div>

					<!-- Sample entries -->
					<div class='mb-3' v-if='current.samples && current.samples.length'>
						<div class='st-section-label'>
							<span v-if='current.samples.length === 1' tt='st_sample_entry'></span>
							<span v-else tt='st_sample_entries'></span>
						</div>
						<ul class='st-sample-list'>
							<li v-for='s in current.samples' :key='s.id'>
								<a v-if='s.ext_url' :href='s.ext_url' target='_blank' class='external'>{{s.ext_name ||
									s.ext_id}}</a>
								<span v-else>{{s.ext_name || s.ext_id}}</span>
								&nbsp;<a :href='"/#/entry/" + s.id' target='_blank'
									style='color:#bbb; font-size:0.82em'>#{{s.id}}</a>
							</li>
						</ul>
					</div>

					<hr style='margin:0.75rem 0'>

					<!-- Wikidata search -->
					<div class='mb-2'>
						<div class='st-section-label' tt='search_wd'></div>
						<div class='input-group input-group-sm mb-2'>
							<input type='text' class='form-control' v-model='search_query'
								@keyup.enter='search_wikidata' tt_placeholder='search_query' />
							<button class='btn btn-outline-primary' @click.prevent='search_wikidata'
									:disabled='searching'>
									<span v-if='searching'>&#8230;</span>
									<span v-else tt='search'></span>
								</button>
						</div>
					</div>

					<!-- Search results -->
					<div v-if='search_results.length > 0' class='mb-3'>
						<div v-for='r in search_results' :key='r.id' class='st-search-result'>
							<a class='btn btn-outline-secondary btn-sm'
								style='flex-shrink:0; font-family:monospace; min-width:5rem'
								:href='"https://www.wikidata.org/wiki/" + r.id' target='_blank'>
								{{r.id}}
							</a>
							<div class='st-result-meta'>
								<div class='st-result-label'>{{r.label}}</div>
								<div class='st-result-desc'>
									<wd-desc autodesc_first='1' :item='r.id.replace(/\\D/g,"")'
										autodesc_fallback='1'></wd-desc>
								</div>
							</div>
							<button class='btn btn-outline-success btn-sm' style='flex-shrink:0; white-space:nowrap'
								@click.prevent='set_q(r.id)' :disabled='saving'>
								&#10003; <span tt='set_q'></span>
							</button>
						</div>
					</div>
					<div v-else-if='searched && !searching' class='mb-3 text-muted' style='font-size:0.9em'
						tt='st_no_results_hint'></div>

					<!-- Manual Q-ID input -->
					<div>
						<div class='st-section-label' tt='st_enter_qid_manually'></div>
						<div class='input-group input-group-sm' style='max-width:260px'>
							<input type='text' class='form-control' v-model='manual_q' @keyup.enter='set_manual_q'
								placeholder='Q12345' />
							<button class='btn btn-outline-success' @click.prevent='set_manual_q'
									:disabled='saving || !manual_q.trim()'>
									<span v-if='saving'>&#8230;</span>
									<span v-else tt='set_q'></span>
								</button>
						</div>
						<div v-if='error_msg' class='text-danger mt-1' style='font-size:0.85em'>
							{{error_msg}}
						</div>
					</div>

				</div><!-- /padding -->
			</div><!-- /card -->

		</div><!-- /main UI -->

	</div>
`
});
