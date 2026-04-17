import { mnm_api, mnm_notify, tt_update_interface, widar } from './store.js';

(function () {
    const s = document.createElement('style');
    s.textContent = `
div.cf_bar {
    display:flex;
    flex-direction:row;
    margin-top:0.2rem;
}
div.cf_bar > div {
    margin-left:1rem;
    white-space:nowrap;
}
span.test_result_value {
    background-color: #BBDAFF;
    font-family:Courier;
    line-height:0;
}
.cf-lang-label {
    display:inline-block;
    font-size:0.75rem;
    font-weight:bold;
    padding:1px 6px;
    border-radius:3px;
    margin-bottom:2px;
}
.cf-lang-lua { background:#00007f; color:#fff; }
.cf-lang-php { background:#777bb3; color:#fff; }
.cf-deprecated {
    opacity:0.6;
    border-left:3px solid #dc3545;
    padding-left:0.5rem;
}
.cf-deprecated-label {
    font-size:0.7rem;
    color:#dc3545;
    font-weight:bold;
    text-transform:uppercase;
    margin-left:0.5rem;
}
`;
    document.head.appendChild(s);
})();

var code_editor_counter = 0;

export default {
    name: 'code-fragment',
    props: ['fragment', 'test_entry', 'is_user_allowed'],
    data: function () {
        return {
            frag: {}, changed: false,
            lua_editor: null, php_editor: null,
            lua_editor_id: '', php_editor_id: '',
            is_logged_in: false,
            test_results: [], show_test_results: false, test_is_running: false, test_successful: false,
            tested_via: ''
        };
    },
    computed: {
        has_lua: function () { return (this.frag.lua || '').trim() !== ''; },
        has_php: function () { return (this.frag.php || '').trim() !== ''; },
        is_new: function () { return !this.frag.id; },
        show_php_readonly: function () { return this.has_php && !this.is_new; }
    },
    created: function () {
        let me = this;
        let frag = JSON.parse(JSON.stringify(me.fragment));
        me.frag = frag;
        me.lua_editor_id = 'lua_editor_' + code_editor_counter;
        me.php_editor_id = 'php_editor_' + code_editor_counter;
        code_editor_counter++;
    },
    updated: function () { tt_update_interface() },
    mounted: async function () {
        let me = this;
        me.is_logged_in = widar.is_logged_in;
        if (typeof ace === 'undefined') {
            await new Promise(function (resolve, reject) {
                var s = document.createElement('script');
                s.src = 'https://mix-n-match.toolforge.org/ace-builds/src-min-noconflict/ace.js';
                s.onload = resolve;
                s.onerror = reject;
                document.head.appendChild(s);
            });
        }
        me.$nextTick(function () {
            // Lua editor (editable)
            var lua_el = document.getElementById(me.lua_editor_id);
            if (lua_el) {
                me.lua_editor = ace.edit(me.lua_editor_id);
                me.lua_editor.session.setMode('ace/mode/lua');
                me.lua_editor.setValue(me.frag.lua || '', -1);
            }
            // PHP editor (read-only legacy view)
            var php_el = document.getElementById(me.php_editor_id);
            if (php_el) {
                me.php_editor = ace.edit(me.php_editor_id);
                me.php_editor.session.setMode('ace/mode/php');
                var php_display = "<?PHP\n" + (me.frag.php || '').trim() + "\n?>";
                me.php_editor.setValue(php_display, -1);
                me.php_editor.setReadOnly(true);
                me.php_editor.renderer.setShowGutter(true);
                me.php_editor.setHighlightActiveLine(false);
                me.php_editor.container.style.opacity = '0.6';
            }
        });
        tt_update_interface();
    },
    methods: {
        get_frag_for_query: function () {
            let me = this;
            let frag = JSON.parse(JSON.stringify(me.frag));
            // Lua is the only editable code (BESPOKE has no editable code)
            if (me.lua_editor) {
                frag.lua = me.lua_editor.getValue();
            }
            return frag;
        },
        saveCode: async function () {
            let me = this;
            let frag = me.get_frag_for_query();
            try {
                let d = await mnm_api('save_code_fragment', {
                    username: widar.getUserName(),
                    catalog: me.test_entry.catalog,
                    fragment: JSON.stringify(frag)
                }, { method: 'POST' });
                mnm_notify('Code fragment saved', 'success');
            } catch (e) {
                mnm_notify('Error while saving', 'danger');
            }
        },
        runTest: async function () {
            let me = this;
            let frag = me.get_frag_for_query();
            me.test_is_running = true;
            me.test_successful = false;
            me.tested_via = '';
            try {
                let d = await mnm_api('test_code_fragment', {
                    username: widar.getUserName(),
                    entry_id: me.test_entry.id,
                    fragment: JSON.stringify(frag)
                }, { method: 'POST' });
                me.test_is_running = false;
                me.tested_via = d.tested_via || '';
                if (d.data == null) {
                    me.test_results = [];
                    me.show_test_results = false;
                    me.test_successful = false;
                    mnm_notify('Test returned no results', 'warning');
                } else {
                    me.test_results = d.data;
                    me.show_test_results = true;
                    me.test_successful = true;
                }
            } catch (e) {
                me.test_is_running = false;
                me.test_results = [];
                me.show_test_results = false;
                mnm_notify('Test failed: ' + (e.message || 'error in code'), 'danger');
            }
        }
    },
    template: `
<div class='mt-2'>
    <div class='card'>
        <div class="card-body">
            <h5 class="card-title">{{frag.function.replace(/_/g,' ')}}</h5>
            <div class="card-text">

                <!-- Lua editor (primary, editable) -->
                <div>
                    <span class="cf-lang-label cf-lang-lua">Lua</span>
                </div>
                <div :id='lua_editor_id' style='width:100%;height:15rem;'></div>

                <!-- PHP editor (legacy, read-only) shown if PHP code exists -->
                <div v-if='show_php_readonly' class='mt-3 cf-deprecated'>
                    <div>
                        <span class="cf-lang-label cf-lang-php">PHP</span>
                        <span class="cf-deprecated-label">deprecated</span>
                    </div>
                    <div :id='php_editor_id' style='width:100%;height:10rem;'></div>
                </div>

                <div style='margin-top:0.2rem;'>
                    <textarea v-model='frag.json' rows='3' style="width:100%;" tt_placeholder='cf_json'></textarea>
                </div>
                <div class='cf_bar'>
                    <div>
                        <label>
                            <input type='checkbox' v-model='frag.is_active' />
                            <span tt='cf_is_active'></span>
                        </label>
                    </div>
                    <div style='width:100%;'>
                        <input v-model='frag.note' tt_placeholder='note' style='width:100%;' />
                    </div>
                    <div v-if='is_user_allowed && typeof test_entry.id!="undefined" && !test_is_running'>
                        <button class='btn btn-outline-success' tt='test' @click.prevent='runTest()'></button>
                    </div>
                    <div v-if='is_user_allowed && test_successful'>
                        <button class='btn btn-outline-primary' tt='save' @click.prevent='saveCode()'></button>
                    </div>
                </div>
            </div>

            <div v-if='test_is_running'>
                <i tt='test_is_running'></i>
            </div>
            <div v-if='show_test_results' style='margin-top:0.5rem;border-top:1px dotted #DDD'>
                <div>
                    <span tt='test_results'></span>
                    <span v-if='tested_via' class='ms-2'>
                        <span class="cf-lang-label" :class="tested_via==='lua' ? 'cf-lang-lua' : 'cf-lang-php'">
                            tested via {{tested_via.toUpperCase()}}
                        </span>
                    </span>
                </div>
                <table v-if='test_results.length>0' class='table'>
                    <tbody>
                        <tr v-for='(result,result_num) in test_results'>
                            <td nowrap>
                                <span v-if='result.action==1'>Add</span>
                                <span v-else-if='result.action==2'>Set</span>
                                <span v-else-if='result.action==3'>Remove</span>
                                <span v-else><i>Unknown action</i></span>
                            </td>
                            <td nowrap>
                                <span v-if='result.target==1'>person dates</span>
                                <span v-else-if='result.target==2'>auxiliary data</span>
                                <span v-else-if='result.target==3'>entry part</span>
                                <span v-else-if='result.target==4'>alias</span>
                                <span v-else-if='result.target==5'>location</span>
                                <span v-else-if='result.target==6'>location text</span>
                                <span v-else><i>Unknown target</i></span>
                            </td>
                            <td>
                                <span v-if='result.action==2 && result.target==3'>
                                    entry
                                    <span v-if='result.data.key=="ext_name"'>name</span>
                                    <span v-else-if='result.data.key=="ext_desc"'>description</span>
                                    <span v-else-if='result.data.key=="type"'>type</span>
                                    <span v-else>{{result.data.key}}</span>
                                    to
                                    <span class='test_result_value'>{{result.data.value}}</span>
                                </span>
                                <span v-else>
                                    <span class='test_result_value'>{{result.data}}</span>
                                </span>
                            </td>
                        </tr>
                    </tbody>
                </table>
                <div v-else>
                    <i tt='no_test_results'></i>
                </div>
            </div>
        </div>
    </div>
</div>
`
};
