import { entryDisplayMixin, editEntryMixin, entryMixin } from './mnm-mixins.js';
import { wd, tt, tt_update_interface, get_specific_catalog, widar } from './store.js';

export default {
	name: 'catalog-entry-multi-match',
	props: ['entry'],
	data: function () { return { loaded: false, ld: {}, display: false, highlight: undefined } },
	mixins: [entryDisplayMixin, editEntryMixin, entryMixin],
	created: function () {
		const me = this;
		//console.log(JSON.parse(JSON.stringify(me.entry)));
		me.loaded = false;
		me.display = false;
		if (!me.entry.multimatch || me.entry.multimatch.length == 0) return;
		if (typeof me.entry.q != 'undefined' && me.entry.user > 0) return; // Already set

		let highlight = [];
		if (typeof me.entry.born != 'undefined') {
			let year = me.entry.born.match(/^(\d{4})/);
			if (year != null) highlight.push(year[0]);
		}
		if (typeof me.entry.died != 'undefined') {
			let year = me.entry.died.match(/^(\d{4})/);
			if (year != null) highlight.push(year[0]);
		}
		if (highlight.length > 0) {
			me.highlight = '\b(' + highlight.join('|') + ')\b';
		}

		me.display = true;
		var titles = [];

		wd.getItemBatch(me.entry.multimatch).then(function () {
			me.ld = {};
			var cat = get_specific_catalog(me.entry.catalog);
			var language = cat ? cat.search_wp : 'en';
			me.entry.multimatch.forEach(function (q) {
				var v = wd.getItem(q);
				if (typeof v == 'undefined') return;
				v = v.raw;

				var o = { label: q, description: '' };
				['label', 'description'].forEach(function (key) {
					var key2 = key + 's';
					if (!v[key2]) return;
					Object.entries(v[key2]).forEach(function (e) { if (key == 'label') o[key] = e[1].value; });
					Object.entries(v[key2]).forEach(function (e) { if (['en', 'de', 'es', 'it', 'fr', 'nl'].includes(e[0])) o[key] = e[1].value; });
					Object.entries(v[key2]).forEach(function (e) { if (e[0] == language) o[key] = e[1].value; });
					Object.entries(v[key2]).forEach(function (e) { if (e[0] == tt.language) o[key] = e[1].value; });
				});
				if (typeof v.claims != 'undefined') {
					var dates = ['', ''];
					if (typeof v.claims.P569 != 'undefined' && typeof v.claims['P569'][0].mainsnak.datavalue != 'undefined') dates[0] = (v.claims['P569'][0].mainsnak.datavalue.value.time.match(/^[\+]{0,1}(.\d+)/))[1];
					if (typeof v.claims.P570 != 'undefined' && typeof v.claims['P570'][0].mainsnak.datavalue != 'undefined') dates[1] = (v.claims['P570'][0].mainsnak.datavalue.value.time.match(/^[\+]{0,1}(.\d+)/))[1];
					if (dates[0] + dates[1] != '') o.description += ' (' + dates[0] + '–' + dates[1] + ')';
				}
				me.ld[q] = o;
			});
			me.loaded = true;
		});
	},
	updated: function () { tt_update_interface() },
	methods: {
		setQ: function (q, skip_wikidata_edit) {
			const me = this;
			me.editing = true;
			me.setEntryQ(me.entry, q, skip_wikidata_edit, me.stopEditing, undefined, { silent: true });
			me.entry.username = widar.getUserName();
			me.entry.q = q;
			me.display = false;
			return false;
		},
		setUserQ: function (e, mq) {
			const me = this;
			e.preventDefault();
			var q = mq.replace(/\D/g, '');
			if (q != '') me.setQ(q);
			return false;
		},
		noneAreCorrect: function (e) {
			let me = this;
			me.display = false;
			e.preventDefault();
			me.removeAllMultimatches(me.entry);
			return false;
		}
	},
	template: `
	<span v-if='display' tt_title='multimatch_candidates' style='margin-left:30px;'>
		<div class="btn-group">
			<button type="button" class="btn btn-light btn-sm dropdown-toggle dropdown-toggle-split"
				data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false">{{entry.multimatch.length}}</button>
			<div class="dropdown-menu dropdown-menu-end" style="padding:2px;max-width:500px;overflow:auto;"
				v-if='loaded'>
				<div v-for='mq in entry.multimatch' :title='mq'
					style='white-space:nowrap;line-height:1;margin-bottom:7pt'>
					<div v-if='typeof ld[mq]!="undefined"'>
						<div>
							<a :href='"https://www.wikidata.org/wiki/"+mq' target='_blank' class='wikidata'
								:style='ld[mq].label.toLowerCase()==entry.ext_name.toLowerCase()?"font-weight:bold":""'>{{ld[mq].label}}</a>
							<small v-if='mq!=ld[mq].label'>[{{mq}}]</small>
							<small>[<a href='#' @click.prevent='setUserQ(\$event,mq)' tt='set_q'
									class='set_user_q'></a>]</small>
						</div>
						<div style='font-size:7pt;max-width:500px;white-space:normal'>
							<span v-if='ld[mq].description.length==0'><i>
									<wd-desc :autodesc_first='entry.type=="Q5"' :item='mq.replace(/\\D/g,"")'
										autodesc_fallback='1'></wd-desc>
									<!--<autodesc :item='mq.replace(/\\D/g,"")' mode='long' :key='entry.id+"_"+mq' :highlight='highlight'></autodesc>-->
								</i></span>
							<span v-else class='catalog-entry-multi-match-desc'>{{ld[mq].description}}</span>
						</div>
					</div>
				</div>
				<div style='white-space:nowrap;line-height:1;margin-bottom:7pt'>
					<a href='#' @click.prevent='noneAreCorrect(\$event)' tt='none_are_correct'></a>
				</div>
			</div>
		</div>
	</span>
`
};
