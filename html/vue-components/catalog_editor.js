import { mnm_api, mnm_fetch_json, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

export default Vue.extend({
    props: ['id'],
    data: function () { return { catalog: {} } },
    created: async function () {
        let me = this;
        await ensure_catalog(me.id);
        me.catalog = get_specific_catalog(me.id);
    },
    updated: function () { tt_update_interface() },
    mounted: function () { tt_update_interface() },
    methods: {
        update_ext_urls: async function () {
            let me = this;
            let prop = 'P' + me.catalog.wd_prop;
            try {
                let d = await mnm_fetch_json('https://www.wikidata.org/w/api.php', {
                    action: 'wbgetentities', format: 'json', origin: '*', ids: prop
                });
                let x = d.entities[prop].claims || {};
                if (typeof x.P1630 == "undefined") {
                    mnm_notify(prop + ' has no formatter URL', 'danger');
                    return;
                }
                let url = '';
                x.P1630.forEach(claim => {
                    if (claim.rank == 'preferred' || (url == '' && claim.rank == 'normal')) url = claim.mainsnak.datavalue.value;
                });
                if (url == '') {
                    mnm_notify(prop + ' has no suitable formatter URL (maybe deprecated only?', 'danger');
                    return;
                }
                await mnm_api('update_ext_urls', {
                    username: widar.getUserName(),
                    url: url,
                    catalog: me.id
                });
                mnm_notify("Done", 'success');
            } catch (e) {
                mnm_notify(e.message, 'danger');
            }
        },
        onSave: async function () {
            const me = this;
            try {
                await mnm_api('edit_catalog', {
                    username: widar.getUserName(),
                    catalog: me.id,
                    data: JSON.stringify(me.catalog)
                }, { method: 'POST' });
                await ensure_catalog(me.id, true);
                mnm_notify('Catalog saved', 'success');
                router.push('/catalog/' + me.id);
            } catch (e) {
                mnm_notify('Save failed: ' + e.message, 'danger');
            }
        }
    },
    template: `
<div class='mt-2'>
	<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{tt: 'catalog_editor'}
	]"></mnm-breadcrumb>
	<catalog-header :catalog="catalog"></catalog-header>
	<h2 tt='catalog_editor'></h2>
	<div v-if='!widar.is_catalog_admin' class="alert alert-warning">
		You are not a catalog admin, so you can't change any of the setting below. If you want to be a catalog admin, ask Magnus!
	</div>
	<form>
		<fieldset :disabled='!widar.is_catalog_admin'>
			<div class="mb-3">
				<label>Catalog name</label>
				<input type="text" class="form-control" v-model='catalog.name' />
			</div>
			<div class="mb-3">
				<label>Catalog description</label>
				<input type="text" class="form-control" v-model='catalog.desc' />
			</div>
			<div class="mb-3">
				<label>Catalog url</label>
				<input type="text" class="form-control" v-model='catalog.url' />
			</div>
			<div class="mb-3">
				<label>Catalog type</label>
				<input type="text" class="form-control" v-model='catalog.type' />
			</div>
			<div class="mb-3">
				<label>Main language</label>
				<input type="text" class="form-control" v-model='catalog.search_wp' />
			</div>
			<div class="mb-3">
				<label>Property/qualifier</label>
				<input type="number" class="form-control" v-model='catalog.wd_prop' placeholder='property' style='width:10rem;display:inline-block;' />
				<input type="number" class="form-control" v-model='catalog.wd_qual' placeholder='qualifier' style='width:10rem;display:inline-block;' />
				<i>(numbers only!)</i>
			</div>
			<div class="mb-3" v-if='typeof catalog.wd_prop!="undefined" && catalog.wd_prop>0'>
				<a href='#' @click.prevent='update_ext_urls'>Update external URLs from property formatter URL and IDs</a> <i style='color:red;'>careful!</i>
			</div>
			<div class="mb-3">
				<label>Active</label>
				<input type="checkbox" class="form-control" v-model='catalog.active' value='1' style='width:2rem;display:inline-block;' />
			</div>
			<div v-if='widar.is_catalog_admin' class="mb-3">
				<button class='btn btn-outline-primary' @click.prevent='onSave'>Save</button>
			</div>
		</fieldset>
	</form>
</div>
`
});
