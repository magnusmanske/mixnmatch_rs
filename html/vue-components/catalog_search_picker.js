import { mnm_api } from './store.js';

function sameIdList(a, b) {
	if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
	for (var i = 0; i < a.length; i++) {
		if ((a[i] && a[i].id) !== (b[i] && b[i].id)) return false;
	}
	return true;
}

(function() {
  const s = document.createElement('style');
  s.textContent = `
.csp-wrapper { position: relative; }
.csp-input { width: 100%; }
.csp-results {
    position: absolute; z-index: 500; left: 0; right: 0; max-height: 300px;
    overflow-y: auto; background: #fff; border: 1px solid #dee2e6;
    border-top: none; border-radius: 0 0 6px 6px; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}
.csp-result-item { cursor: pointer; }
.csp-result-item:hover, .csp-result-item.csp-active { background: #e9ecef; }
.csp-selected-list { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 4px; }
.csp-tag {
    display: inline-flex; align-items: center; gap: 4px; padding: 2px 8px;
    background: #e9ecef; border-radius: 12px; font-size: 0.82rem;
}
.csp-tag-remove { cursor: pointer; color: #666; font-weight: bold; }
.csp-tag-remove:hover { color: #c00; }
`;
  document.head.appendChild(s);
})();

export default {
	name: 'catalog-search-picker',
	props: {
		placeholder: { type: String, default: '' },
		multi: { type: Boolean, default: false },
		linkable: { type: Boolean, default: false },
		input_class: { type: String, default: '' },
		value: { default: null } // v-model: single mode = catalog object or null; multi mode = array of {id,name}
	},
	data: function () {
		return {
			query: '',
			results: [],
			show_results: false,
			highlight_idx: -1,
			search_timer: null,
			selected_list: []
		}
	},
	created: function () {
		if (this.multi && Array.isArray(this.value)) {
			this.selected_list = this.value.slice();
		}
	},
	watch: {
		// The parent may populate `value` after we've already mounted
		// (typical: it resolves catalog info asynchronously in its own
		// `created`, then mutates the array). Without this watch, the
		// picker's pill list is a stale copy taken at mount time.
		// Deep is needed because the parent usually pushes into the
		// same reference rather than assigning a fresh array.
		value: {
			deep: true,
			handler: function (newVal) {
				if (!this.multi) return;
				if (!Array.isArray(newVal)) {
					this.selected_list = [];
					return;
				}
				// Skip the self-echo: after `selectResult` pushes into
				// `selected_list` and emits, the parent re-binds the
				// same content back — syncing when content already
				// matches avoids an extra render.
				if (sameIdList(this.selected_list, newVal)) return;
				this.selected_list = newVal.slice();
			},
		},
	},
	methods: {
		onInput: function () {
			const me = this;
			me.highlight_idx = -1;
			clearTimeout(me.search_timer);
			if (me.query.trim().length < 2) {
				me.results = [];
				me.show_results = false;
				return;
			}
			me.search_timer = setTimeout(function () { me.doSearch(); }, 200);
		},
		doSearch: async function () {
			const me = this;
			try {
				var d = await mnm_api('search_catalogs', { q: me.query.trim(), limit: 15 });
				var selected_ids = {};
				if (me.multi) {
					me.selected_list.forEach(function (s) { selected_ids[s.id] = true; });
				}
				me.results = (d.data || []).filter(function (r) { return !selected_ids[r.id]; });
				me.show_results = me.results.length > 0;
				me.highlight_idx = -1;
			} catch (e) {
				console.error('Catalog search failed', e);
			}
		},
		onKeydown: function (e) {
			const me = this;
			if (e.key === 'ArrowDown') {
				e.preventDefault();
				if (me.highlight_idx < me.results.length - 1) me.highlight_idx++;
			} else if (e.key === 'ArrowUp') {
				e.preventDefault();
				if (me.highlight_idx > 0) me.highlight_idx--;
			} else if (e.key === 'Enter') {
				e.preventDefault();
				if (me.highlight_idx >= 0 && me.highlight_idx < me.results.length) {
					me.selectResult(me.results[me.highlight_idx]);
				}
			} else if (e.key === 'Escape') {
				me.show_results = false;
			}
		},
		onFocus: function () {
			if (this.results.length > 0) this.show_results = true;
		},
		onBlur: function () {
			// Delay to allow mousedown on result to fire first
			const me = this;
			setTimeout(function () { me.show_results = false; }, 200);
		},
		selectResult: function (r) {
			const me = this;
			if (me.multi) {
				me.selected_list.push({ id: r.id, name: r.name });
				me.$emit('input', me.selected_list.slice());
				me.$emit('change', me.selected_list.slice());
				me.query = '';
				me.results = [];
				me.show_results = false;
			} else {
				me.query = r.name;
				me.show_results = false;
				me.$emit('input', r);
				me.$emit('select', r);
			}
		},
		removeSelected: function (id) {
			const me = this;
			me.selected_list = me.selected_list.filter(function (s) { return s.id != id; });
			me.$emit('input', me.selected_list.slice());
			me.$emit('change', me.selected_list.slice());
		}
	},
	template: `
<div class='csp-wrapper'>
    <input type='text' class='form-control csp-input' :class='input_class'
        :placeholder='placeholder||"Search catalogs..."'
        v-model='query' @input='onInput' @keydown='onKeydown' @focus='onFocus' @blur='onBlur' />
    <div v-if='show_results && results.length > 0' class='csp-results'>
        <div v-for='(r,idx) in results' :key='r.id'
            :class="'csp-result-item'+(idx===highlight_idx?' csp-active':'')"
            @mousedown.prevent='selectResult(r)'>
            <catalog-preview :catalog='r'></catalog-preview>
        </div>
    </div>
    <div v-if='multi && selected_list.length > 0' class='csp-selected-list'>
        <span v-for='s in selected_list' :key='s.id' class='csp-tag'>
            <router-link v-if='linkable' :to="'/catalog/'+s.id">{{s.name}}</router-link>
            <span v-else>{{s.name}}</span>
            <span class='csp-tag-remove' @click='removeSelected(s.id)'>&times;</span>
        </span>
    </div>
</div>
`
};
