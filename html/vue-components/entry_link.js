import { entryDisplayMixin } from './mnm-mixins.js';

export default {
	name: 'entry-link',
	mixins: [entryDisplayMixin],
	props: ['entry'],
	template: `
	<span>
		<span v-if='entry.ext_url.length>0'><a :href="entry.ext_url" class="external"
				target="_blank">{{entry.ext_name|decodeEntities|removeTags|miscFixes}}</a></span>
		<span v-else>{{entry.ext_name|decodeEntities|removeTags|miscFixes}}</span>
	</span>
`
};
