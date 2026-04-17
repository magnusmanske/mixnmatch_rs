import { mnm_api, mnm_notify, ensure_catalog, get_specific_catalog, tt_update_interface, widar } from './store.js';

export default Vue.extend({
    props: ['id'],
    data: function () { return { text: '', catalog: {} } },
    created: async function () {
        const me = this;
        await ensure_catalog(me.id);
        me.catalog = get_specific_catalog(me.id);
    },
    updated: function () { tt_update_interface() },
    mounted: function () { tt_update_interface() },
    methods: {
        onSave: async function () {
            const me = this;
            try {
                await mnm_api('add_aliases', {
                    username: widar.getUserName(),
                    catalog: me.id,
                    text: me.text
                }, { method: 'POST' });
                await ensure_catalog(me.id, true);
                mnm_notify('Aliases added', 'success');
                router.push('/catalog/' + me.id);
            } catch (e) {
                mnm_notify('Failed: ' + e.message, 'danger');
            }
        }
    },
    template: `
<div class='mt-2'>
	<mnm-breadcrumb v-if='catalog && catalog.id' :crumbs="[
		{text: catalog.name, to: '/catalog/'+catalog.id},
		{tt: 'aliases'}
	]"></mnm-breadcrumb>
	<catalog-header :catalog="catalog"></catalog-header>
	<h2 tt='aliases'></h2>
	<div v-if='!(widar.is_catalog_admin||widar.userinfo.name==catalog.username)' class="alert alert-warning" tt='no_catalog_admin_or_owner'></div>
	<form>
		<fieldset :disabled='!(widar.is_catalog_admin||widar.userinfo.name==catalog.username)'>
			<div tt='aliases_blurb'></div>
			<div>
				<textarea v-model='text' style='width:100%' rows=10 tt_placeholder='ph_paste_text_here'></textarea>
			</div>
			<div v-if='(widar.is_catalog_admin||widar.userinfo.name==catalog.username)' class="mb-3">
				<button class='btn btn-outline-primary' @click.prevent='onSave' tt='save'></button>
				<span tt='aliases_save_note'></span>
			</div>
		</fieldset>
	</form>
</div>
`
});
