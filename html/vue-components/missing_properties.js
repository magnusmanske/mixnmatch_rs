import { mnm_api, mnm_notify, tt_update_interface, widar } from './store.js';

(function () {
	const s = document.createElement('style');
	s.textContent = `
span.status_cb {
    margin-right: 1rem;
}
span.header_box {
    margin-bottom: 0.2rem;
    border-bottom: 1px solid #DDD;
}
span.jump {
    margin-right: 0.5rem;
}
`;
	document.head.appendChild(s);
})();

export default Vue.extend({
	data: function () {
		return {
			loaded: false, props: [], props_filtered: [], error: '', sort: 'property_name',
			sort_reverse: false, last_id_clicked_on: 0, rows: [], start: 0, max: 25, has_more: false,
			status_stats: {},
			statuses: { 'NO_CATALOG': true, 'HAS_CATALOG': false, 'NOT_SUITABLE': false, 'DIFFICULT': false, 'BROKEN': false },
			status_class: { 'NO_CATALOG': 'bg-info', 'HAS_CATALOG': 'bg-success', 'NOT_SUITABLE': 'bg-warning', 'DIFFICULT': 'bg-secondary', 'BROKEN': 'bg-danger' },
		};
	},
	created: async function () {
		let self = this;
		try {
			let d = await mnm_api('get_missing_properties');
			self.props_filtered = [];
			self.status_stats = {};
			Object.keys(self.statuses).forEach(function (key) {
				self.status_stats[key] = 0;
			});
			self.props = d.data;
			self.props.forEach(function (row) {
				row.property_name_lc = row.property_name.toLowerCase();
				row.property_num *= 1;
				if (typeof self.status_stats[row.status] !== 'undefined') self.status_stats[row.status] += 1;
			});
			self.loaded = true;
			self.filter_props();
		} catch (e) {
			self.error = e.message || 'ERROR';
			self.loaded = true;
		}
	},
	updated: function () { tt_update_interface() },
	mounted: function () { tt_update_interface() },
	methods: {
		jump: function (new_pos) {
			this.start = new_pos;
			this.render();
		},
		was_clicked_on: function (row) {
			if (this.last_id_clicked_on != row.id) return '';
			return 'background-color:#8EB4E6 !important;';
		},
		get_scraper_link: function (row) {
			let url = "https://mix-n-match.toolforge.org/#/scraper/new?property=P" + row.property_num;
			if (row.default_type == 'Q5') url += "&type=biography";
			return url;
		},
		clicked_on: function (row) {
			this.last_id_clicked_on = row.id;
		},
		on_status_change: async function (row) {
			let note = prompt("Add a note (optional):", "");
			if (note === null) note = '';
			row.note = note;
			try {
				await mnm_api('set_missing_properties_status', {
					row_id: row.id,
					status: row.status,
					note: note,
					username: widar.getUserName(),
				});
			} catch (e) {
				mnm_notify(e.message || 'Request failed', 'danger');
			}
		},
		sort_list: function (row1, row2) {
			if (this.sort == 'property_name') {
				if (row1.property_name_lc === row2.property_name_lc) return 0;
				return row1.property_name_lc < row2.property_name_lc ? -1 : 1;
			} else if (this.sort == 'property_id') {
				return row1.property_num - row2.property_num;
			}
		},
		filter_props: function () {
			let self = this;
			self.start = 0;
			let cache = self.props
				.filter(function (row) {
					return self.statuses[row.status];
				});
			cache.sort(self.sort_list);
			if (self.sort_reverse) cache.reverse();
			self.props_filtered = cache;
			self.render();
		},
		render: function () {
			let self = this;
			let start = self.start;
			let end = start + self.max - 1;
			self.rows = self.props_filtered.slice(start, end);
			self.has_more = (end <= self.props_filtered.length);
			self.last_id_clicked_on = 0;
		}
	},
	template: `
<div class='mt-2'>
    <mnm-breadcrumb :crumbs="[{text: 'Missing properties'}]"></mnm-breadcrumb>
    <div v-if='loaded && error!=""'>
        <b>{{error}}</b>
    </div>
    <div v-if='loaded && error==""'>
        <div class='header_box'>
            <span v-for='(value,status) in statuses' class='status_cb'>
                <label>
                    <input type='checkbox' v-model='statuses[status]' @change='filter_props'/>
                    {{status.toLowerCase().replace(/_/g,' ')}}
                </label>
            </span>
            <span class='status_cb'>
                <label>
                    <input type='radio' v-model='sort' value='property_name' @change='filter_props' />
                    <span tt='by_alpha'></span>
                </label>
                <label>
                    <input type='radio' v-model='sort' value='property_id' @change='filter_props'/>
                    <span tt='by_id'></span>
                </label>
                <label>
                    <input type='checkbox' v-model='sort_reverse' @change='filter_props' />
                    <span tt='descending'></span>
                </label>
            </span>
            <div class="progress">
                <div v-for='(count,key) in status_stats' role="progressbar" :class="'progress-bar '+status_class[key]" :style="{'white-space':'nowrap',width:100*count/props.length+'%'}" :title="key.toLowerCase().replace(/_/g,' ')+' :'+count">
                    {{Math.floor(100*count/props.length)+'%'}}
            </div>
        </div>
        <div v-if='rows.length>0'>
            <div class='header_box'>
                <span v-if='start>0' class='jump'>
                    <a href='#' @click.prevent='jump(start-max)'>{{start-max+1}}&mdash;{{start}}</a>
                </span>
                <span>
                    {{start+1}}&mdash;{{start+max}}
                </span>
                <span v-if='has_more' class='jump'>
                    <a href='#' @click.prevent='jump(start+max)'>{{start+max+1}}&mdash;{{start+2*max}}</a>
                </span>
                <span class='jump'>
                    (<span tt='total'></span> {{props_filtered.length}})
                </span>
            </div>
            <table class='table table-striped'>
                <thead>
                    <tr>
                        <th>Property</th>
                        <th>Name</th>
                        <th>Default type</th>
                        <th>Status</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
                    <tr v-for='row in rows' :style='was_clicked_on(row)' @click='clicked_on(row)'>
                        <td>
                            <a :href='"https://www.wikidata.org/wiki/Property:P"+row.property_num' target='_blank' class='wikidata'>{{"P"+row.property_num}}</a>
                        </td>
                        <td>
                            {{row.property_name}}
                        </td>
                        <td>
                            {{row.default_type}}
                        </td>
                        <td>
                            <span v-if='widar.is_logged_in'>
                                <select @change='on_status_change(row)' v-model='row.status'>
                                    <option v-for='(value,status) in statuses'
                                        :value='status'>
                                        {{status.toLowerCase().replace(/_/g,' ')}}
                                    </option>
                                </select>
                            </span>
                            <span v-else>
                                <i>{{row.status.toLowerCase().replace(/_/g,' ')}}</i>
                            </span>
                            <small>{{row.note}}</small>
                        </td>
                        <td style='font-size:8pt'>
                            <a :href='get_scraper_link(row)' target='_blank' tt='new_scraper'></a>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        <div v-else>
            <i>No results for this query</i>
        </div>
    </div>
    <div v-else>
        <i tt='loading'></i>
    </div>
</div>
`
});
