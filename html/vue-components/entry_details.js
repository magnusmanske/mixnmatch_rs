import { editEntryMixin, entryMixin } from './mnm-mixins.js';
import { ensure_catalog, get_specific_catalog, tt_update_interface, tt } from './store.js';

export default {
	name: 'entry-details',
	mixins: [editEntryMixin, entryMixin],
	props: ['entry', 'random', 'random_base', 'show_catalog'],
	data: function () { return { catalog: {} } },
	created: async function () {
		await ensure_catalog(this.entry.catalog);
		this.catalog = get_specific_catalog(this.entry.catalog);
		tt_update_interface();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface() },
	methods: {
		loadRandomData: function () {
			this.$emit('random_entry_button_clicked');
		}
	},
	template: `
	<div>

		<div class="card mb-3">
			<div class="card-body">
				<div class='d-flex justify-content-between align-items-start flex-wrap gap-2 mb-2'>
					<h4 class="card-title mb-0">
						{{entry.ext_name|decodeEntities|removeTags|miscFixes}}
						<router-link class='ms-1' style='font-size:0.85em;' :to='"/search/"+filteredName()'
							tt_title='search'>&#x1F50E;</router-link>
					</h4>
					<div class='d-flex gap-2 flex-shrink-0'>
						<catalog-entry-multi-match :entry='entry'></catalog-entry-multi-match>
						<button v-if='random' class='btn btn-outline-primary btn-sm load-random-entry' @click.prevent='loadRandomData'
							tt='next_entry'></button>
					</div>
				</div>
				<div class="card-text">
					<table class='table table-striped'>
						<tbody>
							<tr v-if='show_catalog'>
								<th nowrap tt='catalog_name'></th>
								<td style='width:100%'>
									<router-link :to='"/catalog/"+entry.catalog'>
										{{get_catalog(entry.catalog).name|decodeEntities|removeTags|miscFixes}}
									</router-link>
								</td>
							</tr>
							<tr>
								<th nowrap tt='entry'></th>
								<td style='width:100%'><router-link :to='"/entry/"+entry.id'>{{entry.id}}</router-link>
								</td>
							</tr>
							<tr>
								<th nowrap tt='catalog_id'></th>
								<td style='width:100%'>
									<span v-if='entry.ext_url.length>0'><a :href="entry.ext_url" class="external"
											target="_blank">{{entry.ext_id}}</a></span>
									<span v-else>{{entry.ext_id}}</span>
								</td>
							</tr>

							<tr v-if='typeof entry.aliases!="undefined"'>
								<th nowrap tt='aliases'></th>
								<td style='width:100%'>
									<table class='table table-sm' style='width:auto'>
										<tr v-for='a in entry.aliases'>
											<td>
												{{a.language}}
											</td>
											<td>
												{{a.label}}
											</td>
										</tr>
									</table>
								</td>
							</tr>

							<tr>
								<th nowrap tt='catalog_desc'></th>
								<td style='width:100%'>
									{{entry.ext_desc|decodeEntities|removeTags|miscFixes}}
									<translator :text="entry.ext_desc" :from="catalog.search_wp" :to="tt.language">
									</translator>
								</td>
							</tr>

							<tr v-if='typeof entry.descriptions!="undefined"'>
								<th tt='descriptions'></th>
								<td style='width:100%'>
									<table class='table table-sm' style='width:auto'>
										<tr v-for='a in entry.descriptions'>
											<td>
												{{a.language}}
											</td>
											<td>
												{{a.label}}
											</td>
										</tr>
									</table>
								</td>
							</tr>

							<tr v-if='entry.type!="" && entry.type!="unknown"'>
								<th nowrap tt='type'></th>
								<td style='width:100%'>
									<span v-if='(entry.type||"").match(/^Q\\d+$/)'>
										<wd-link :item='entry.type.substr(1)' :key='entry.type' smallq=1 />
									</span>
									<span v-else>{{entry.type|decodeEntities|removeTags|miscFixes}}</span>
								</td>
							</tr>

							<tr v-if='typeof entry.born!="undefined" || typeof entry.died!="undefined"'>
								<th nowrap tt='person_dates'></th>
								<td style='width:100%'>
									<span v-if='typeof entry.born!="undefined"'>{{entry.born}}</span>
									&nbsp;&ndash;&nbsp;
									<span v-if='typeof entry.died!="undefined"'>{{entry.died}}</span>
								</td>
							</tr>

							<tr v-if='typeof entry.lat!="undefined"'>
								<th nowrap tt='location'></th>
								<td style='width:100%'>
									<a :href='"https://wikishootme.toolforge.org/#lat="+entry.lat+"&lng="+entry.lon+"&zoom=16&layers=commons,mixnmatch,wikidata_image,wikidata_no_image,wikipedia"'
										target='_blank' class="external">
										{{entry.lat}}/{{entry.lon}}
									</a>
								</td>
							</tr>

							<!-- <tr v-if='typeof entry.image_url!="undefined" && entry.image_url!=""'>
<th nowrap tt='external_image'></th>
<td style='width:100%'>
	<img :src='entry.image_url[0]' width='300px' style='max-height:250px;object-fit: contain;' />
</td>
</tr> -->

							<tr v-if='typeof entry.aux!="undefined"'>
								<th nowrap tt='aux_data'></th>
								<td style='width:100%'>
									<div v-for='a in entry.aux' v-if='a.aux_p==18' style='float:right;clear: right'>
										<commons-thumbnail :filename='a.aux_name' width='300'></commons-thumbnail>
									</div>
									<table class='table table-sm' style='width:auto'>
										<tr v-for='a in entry.aux'>
											<td>
												<wd-link :item='"P"+a.aux_p' :key='"P"+a.aux_p'></wd-link>
											</td>
											<td>
												<span v-if='/^Q\\d+$/.test(a.aux_name)'>
													<wd-link :item='a.aux_name' smallq=1></wd-link>
												</span>
												<span v-else>
													{{a.aux_name}}
												</span>
											</td>
										</tr>
									</table>
								</td>
							</tr>

							<tr v-if='typeof entry.relation!="undefined"'>
								<th nowrap tt='relation_data'></th>
								<td style='width:100%'>
									<table class='table table-sm' style='width:auto'>
										<tr v-for='r in entry.relation'>
											<td>
												<wd-link :item='"P"+r.property' :key='"P"+r.property'></wd-link>
											</td>
											<td>
												<router-link
													:to='"/entry/"+r.id'>{{r.ext_name|decodeEntities|removeTags|miscFixes}}</router-link>
												<small>{{r.ext_desc|decodeEntities|removeTags|miscFixes}}</small>
											</td>
										</tr>
									</table>
								</td>
							</tr>

							<tr v-if="entry.q!=null">
								<th tt='matched_to'></th>
								<td>
									<div style='float:right;text-align:center'>
										<div v-if='entry.user==0' style='margin-bottom:5px;'><button
												class='btn btn-outline-success' style='margin-left:20px;'
												@click.prevent='confirmEntryQ(entry)' tt='confirm'></button></div>
										<button class='btn btn-outline-danger' style='margin-left:20px;'
											@click.prevent='removeEntryQ(entry)' tt='remove'></button>
									</div>
									<div><wd-link :item='entry.q' :key='entry.q' smallq=1></wd-link></div>
									<div>
										<wd-desc :autodesc_first='entry.type=="Q5"' :item='entry.q'
											autodesc_fallback='1'></wd-desc>
										<!--<autodesc :item='entry.q'  :key='entry.q'/>-->
									</div>
								</td>
							</tr>
							<tr v-if="entry.username!=null && typeof entry.username != 'undefined'">
								<th tt='matched_by'></th>
								<td>
									<userlink :username="entry.username" />
								</td>
							</tr>
							<tr v-if="entry.q!=null">
								<th tt='timestamp'></th>
								<td>
									<timestamp :ts='entry.timestamp' />
								</td>
							</tr>

						</tbody>
					</table>
				</div>
			</div>
		</div>

	</div>
`
};
