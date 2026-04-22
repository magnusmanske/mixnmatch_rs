import { entryDisplayMixin, editEntryMixin, entryMixin } from './mnm-mixins.js';
import { wd, tt, tt_update_interface, get_specific_catalog, widar } from './store.js';

export default {
	name: 'entry-list-item',
	mixins: [entryDisplayMixin, editEntryMixin, entryMixin],
	props: ['entry', 'show_catalog', 'show_permalink', 'show_checkbox', 'radio_name', 'rc', 'twoline', 'hide_remove_on_automatch', 'setq'],
	data: function () { return { editing: false, distance: undefined }; },
	mounted: function () {
		let self = this;
		if (typeof self.entry.lat != 'undefined') {
			let q = 'Q' + self.entry.q;
			wd.getItemBatch([q]).then(function () {
				let item = wd.getItem(q);
				if (typeof item != 'undefined') {
					let locations = item.getClaimsForProperty('P625');
					if (locations.length > 0) {
						let location = locations[0];
						let coords = item.getSnakObject(location.mainsnak);
						let lat1 = coords.latitude * 1;
						let lon1 = coords.longitude * 1;
						let lat2 = self.entry.lat * 1;
						let lon2 = self.entry.lon * 1;
						let distance = self.getDistance(lat1, lon1, lat2, lon2);
						if (distance < 1000) distance = Math.round(distance) + 'm';
						else if (distance < 10000) distance = Math.round(distance / 100) / 10 + 'km';
						else distance = Math.round(distance / 1000) + 'km';
						self.distance = distance;
					}
				}
			});
		}
		tt_update_interface();
	},
	updated: function () { tt_update_interface(); },
	methods: {
		getDistance: function (lat1, lon1, lat2, lon2) {
			const R = 6371e3;
			const φ1 = lat1 * Math.PI / 180;
			const φ2 = lat2 * Math.PI / 180;
			const Δφ = (lat2 - lat1) * Math.PI / 180;
			const Δλ = (lon2 - lon1) * Math.PI / 180;
			const a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
				Math.cos(φ1) * Math.cos(φ2) *
				Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
			const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
			return R * c;
		},
		canCreateNewWikidataItem: function () {
			const me = this;
			if (typeof get_specific_catalog(me.entry.catalog) == 'undefined') return false;
			var cat = get_specific_catalog(me.entry.catalog);
			if (typeof cat.wd_prop != 'undefined' && cat.wd_qual == null && !me.entry.ext_id.match(/^fake_id_/)) return true;
			return false;
		},
		stopEditing: function () {
			this.editing = false;
		},
		setQ: function (q, skip_wikidata_edit) {
			const me = this;
			me.editing = true;
			me.setEntryQ(me.entry, q, skip_wikidata_edit, me.stopEditing, undefined, { silent: true });
			return false;
		},
		confirmQ: function () {
			const me = this;
			var q = ('' + me.entry.q).replace(/\D/g, '');
			if (q == '') return false;
			me.setQ(q);
			return false;
		},
		setUserQ: function (e) {
			const me = this;
			e.preventDefault();
			var reply = prompt(tt.t('enter_q_number'), "");
			if (reply === null) return false;
			var q = reply.replace(/\D/g, '');
			if (q == '') return false;
			me.setQ(q);
			return false;
		},
		setNA: function (e) {
			e.preventDefault();
			// Pass the N/A sentinel (q=0) and skip the Wikidata edit
			// explicitly. The previous `setQ(e, 0)` sent the Event object
			// as q, which setEntryQ then coerced to NaN, which the server
			// parsed as the default -1 — leading to "Out of range value
			// for column 'q'" on the reference_fixer insert (that column
			// is INT UNSIGNED).
			return this.setQ(0, true);
		},
		newItem: function (e) {
			e.preventDefault();
			const me = this;
			me.editing = true;
			me.newItemForEntry(me.entry, me.stopEditing, undefined, undefined, { silent: true });
			return false;
		},
		removeQ: function (e) {
			e.preventDefault();
			const me = this;
			me.editing = true;
			me.removeEntryQ(me.entry, me.stopEditing);
			return false;
		},
		removeAllQ: function (e) {
			e.preventDefault();
			const me = this;
			me.editing = true;
			me.removeEntryAllQ(me.entry, me.stopEditing);
			return false;
		},
		wikipediaLanguage: function () {
			if (typeof get_specific_catalog(this.entry.catalog) == 'undefined') return 'en';
			return get_specific_catalog(this.entry.catalog).search_wp;
		},
		wikipediaSearch: function () {
			return "https://" + this.wikipediaLanguage() + ".wikipedia.org/w/index.php?title=Special%3ASearch&search=" + this.getSearchString(false);
		},
		wikidataSearch: function () {
			let query = this.entry.ext_name;
			if (this.entry.type == 'Q5') query += " haswbstatement:P31=Q5";
			let ret = "https://www.wikidata.org/w/index.php?button=&title=Special%3ASearch&search=" + encodeURIComponent(query);
			return ret;
		},
		// True when this is a Q5 whose ext_name contains at least one
		// initial, e.g. 'H.M.Manske' or 'D. S. Felix'. Used to decide
		// whether to offer the infernal initial-search button. The
		// legacy 'person' synonym was removed — writes now normalise
		// to Qxxx; existing rows are migrated via mnm_db_migrations.sql.
		hasInitials: function () {
			if (!this.entry || this.entry.type !== 'Q5') return false;
			let name = this.entry.ext_name || '';
			return /\b[A-Z]\./.test(name);
		},
		initialSearchUrl: function () {
			return 'https://wd-infernal.toolforge.org/initial_search/'
				+ encodeURIComponent(this.entry.ext_name || '')
				+ '?format=html';
		},
		onSetQevent: function () {
			this.$emit('onsetq', this.entry.q);
		},
		get_catalog: function (catalog_id) {
			return get_specific_catalog(catalog_id);
		}
	},
	template: `<div v-if='typeof entry != "undefined"' :class='"entry_row"+(editing?" inactive":"")' :entry='entry.id' style="position:relative">
		<div v-if="editing" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);z-index:10">
			<span class="spinner-border spinner-border-sm text-primary" role="status"></span>
		</div>

		<div v-if='rc' class="rc-row"> <!-- RECENT CHANGES -->
			<div class='entry_cell_left'>
				<timestamp :ts="entry.timestamp" />
			</div>
			<div class="rc-cell-entry">
				<span v-if='show_permalink'><router-link :to='"/entry/"+entry.id'>#</router-link> </span>
				<span v-if='show_catalog && typeof get_catalog(entry.catalog)!="undefined"'><router-link
						:to='"/catalog/"+entry.catalog'><small>{{get_catalog(entry.catalog).name}}</small></router-link>:</span>
				<entry-link :entry='entry'></entry-link>
					<small>({{entry.ext_id|decodeEntities}})</small>
			</div>
			<div class="rc-cell-event">
				<div v-if="entry.event_type=='match'">
					<span v-if='entry.q==0' tt='not_applicable' style='color:#DFDF00'></span>
					<span v-else><span tt='matched_to'></span> <wd-link :item='entry.q' :key='entry.q' /></span>
				</div>
				<div v-if="entry.event_type=='remove_q'"><span style='color:red' tt='wikidata_was_unlinked'></span>
				</div>
			</div>
			<div class='entry_cell_right'>
				By
				<userlink :username='entry.username' :user_id='entry.user' />
			</div>
		</div>

		<div v-else :key='entry.id'> <!-- non-RC-->
			<div class="row"> <!-- begin sub-row -->

				<!--left-->
				<div class='entry_cell_left col-sm-12 col-md-2' style='display: flex;'>
					<div v-if='show_checkbox || radio_name' class="check-and-radio">
						<div v-if='show_checkbox'>
							<input v-if='entry.q==null || entry.user==0' type='checkbox'
								class='entry-list-item-checkbox' :entry='entry.id' />
						</div>
						<div v-if='radio_name'>
							<input v-if='entry.q==null || entry.user==0' type='radio'
								class='entry-list-item-default-entry' :value='entry.id' :name='radio_name' />
						</div>
					</div>
					<div style="display: inline-block; position: relative;">
						<span v-if='show_permalink && !show_checkbox' class="permalink-hover d-none d-md-inline"><router-link
								:to='"/entry/"+entry.id'>#</router-link> </span>
						<span v-if='show_catalog && typeof get_catalog(entry.catalog)!="undefined"'><router-link
								:to='"/catalog/"+entry.catalog'><small>{{get_catalog(entry.catalog).name}}</small></router-link>:</span>
						<br v-if='twoline' />
						<span v-if='show_permalink && show_checkbox' class="permalink-hover-inline d-none d-md-inline-block"><router-link
								:to='"/entry/"+entry.id' class='text-muted' style='font-size:0.8em'>#</router-link></span>
						<entry-link :entry='entry'></entry-link>
						<span v-if='show_permalink' class="d-inline d-md-none"><router-link
								:to='"/entry/"+entry.id' class='text-muted' style='font-size:0.8em;margin-left:4px'>#</router-link></span>
					</div>
				</div>

				<!--middle-->
				<div class='entry_cell_desc col-sm-12 col-md-7'>
					{{entry.ext_desc|decodeEntities|removeTags|miscFixes}}
					<translator :text="entry.ext_desc" :from="wikipediaLanguage()" :to="tt.language"></translator>
				</div>

				<!--right-->
				<div class='entry_cell_right col-sm-12 col-md-3' style="position:relative;font-size:0.8rem;">
					<div v-if='entry.q==null'>
						<i tt="not_matched"></i>
						<catalog-entry-multi-match :entry='entry' style='position:absolute;top:0;right:0'></catalog-entry-multi-match>
					</div>
					<div v-else-if='entry.user==0'>
						<userlink username='automatic' :user_id='0' :catalog_id='entry.catalog' />
						<catalog-entry-multi-match :entry='entry' style='position:absolute;top:0;right:0'></catalog-entry-multi-match>
					</div>
					<div v-else-if='entry.user==3'><userlink username='Automatic name/date matcher' :user_id='3' :catalog_id='entry.catalog' /></div>
					<div v-else-if='entry.user==4'><userlink username='Auxiliary data matcher' :user_id='4' :catalog_id='entry.catalog' /></div>
					<div v-else>By
						<userlink :username='entry.username' :user_id='entry.user' />
					</div>
				</div>

			</div>
			<!-- new sub-row -->

			<div v-if='entry.q==null' class="second-row row">
				<div class='entry_cell_left col-sm-12 col-md-2'></div>
				<div class='entry_cell_desc col-sm-12 col-md-7'>
					<div class='d-flex flex-wrap gap-1 align-items-center'>
						<small class='text-muted me-1' tt='search'></small>
						<div class='btn-group btn-group-sm'>
							<a target='_blank' class='btn btn-outline-secondary mnm-action-btn' :href='wikidataSearch()' tt='search_wd'></a>
							<a v-if='typeof get_catalog(entry.catalog)!="undefined"' target='_blank' class='btn btn-outline-secondary mnm-action-btn'
								:href='wikipediaSearch()' tt='search_wikipedia' :tt1='get_catalog(entry.catalog).search_wp'></a>
							<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
								:href='"https://www.google.com/search?q="+getSearchString()+"+site%3Awikipedia.org"'
								tt='google_wikipedia'></a>
							<a target='_blank' class='btn btn-outline-secondary mnm-action-btn'
								:href='"https://www.google.com/search?q="+getSearchString()+"+site%3Awikidata.org"'
								tt='google_wikidata'></a>
							<a v-if='hasInitials()' target='_blank' class='btn btn-outline-secondary mnm-action-btn'
								:href='initialSearchUrl()' title='Search Wikidata for people whose names expand to these initials'>Initial search</a>
						</div>
					</div>
				</div>
				<div class='entry_cell_right col-sm-12 col-md-3'>
					<div v-if='widar.is_logged_in' class='btn-group btn-group-sm'>
						<button class='btn btn-outline-success mnm-action-btn' @click.prevent='setUserQ' tt='set_q'></button>
						<button v-if="canCreateNewWikidataItem()" class='btn btn-outline-danger mnm-action-btn' @click.prevent='newItem' tt='new_item'></button>
						<button class='btn btn-outline-warning mnm-action-btn' @click.prevent='setNA' tt='n_a'></button>
					</div>
				</div>
			</div>
			<div v-if='entry.q==0' class="second-row row">
				<div class='entry_cell_joined col-sm-12 col-md-9'><i tt="not_applicable"></i></div>
				<div class='entry_cell_right col-sm-12 col-md-3'>
					<button class='btn btn-outline-danger btn-sm mnm-action-btn' @click.prevent='removeQ' tt="remove"></button>
				</div>
			</div>
			<div v-if='entry.q<0' class="second-row row">
				<div class='entry_cell_joined col-sm-12 col-md-9'><i tt="no_wd"></i></div>
				<div class='entry_cell_right col-sm-12 col-md-3'>
					<button class='btn btn-outline-danger btn-sm mnm-action-btn' @click.prevent='removeQ' tt="remove"></button>
				</div>
			</div>
			<div v-if='entry.q>0' :key='entry.q' class="second-row row">
				<div class="entry_cell_left col-sm-12 col-md-2" style="display:flex;">
					<div v-if='show_checkbox' class='check-and-radio'>&nbsp;</div>
					<wd-link :item='entry.q' :key='entry.q' smallq=1></wd-link>
					<small v-if='setq==1' style='white-space: nowrap;'>[<a href='#'
							@click.prevent='onSetQevent'>&#x2191;</a>]</small>
				</div>
				<div class="entry_cell_desc col-sm-12 col-md-7">
					<wd-desc :autodesc_first='entry.type=="Q5"' :item='entry.q' autodesc_fallback='1'></wd-desc>
					<span v-if='typeof distance!="undefined"'
						:style="'color:'+(distance.includes('km')?'#c0392b':'#58d68d')" :tt_title='"distance"'>
						[{{distance}}]</span>
				</div>
				<div class="entry_cell_right col-sm-12 col-md-3">
					<div class='btn-group btn-group-sm'>
						<button v-if='entry.user==0' class='btn btn-outline-success mnm-action-btn' @click.prevent='confirmQ' tt="confirm"></button>
						<button v-if="!hide_remove_on_automatch || entry.user!=0" class='btn btn-outline-danger mnm-action-btn' @click.prevent='removeQ' tt="remove"></button>
						<button v-if="!hide_remove_on_automatch && entry.user==0" class='btn btn-outline-danger mnm-action-btn' @click.prevent='removeAllQ' tt="remove_all" tt_title='t_remove_all'></button>
					</div>
				</div>
			</div>

		</div> <!-- end non-RC-->

	</div>`
};
