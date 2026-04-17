import { mnm_fetch_json, tt_update_interface } from './store.js';

let languages_loaded = false;
let translator_iso;
let apertium_languages;
let translation_cache = [];

export default {
  name: 'translator',
  props: ['text', 'from', 'to'],
  data: function () { return { translated: false, translation: '', note: '' } },
  created: function () {
    const me = this;
    me.load_languages();
  },
  updated: function () { tt_update_interface() },
  mounted: function () { tt_update_interface() },
  methods: {
    load_languages: async function () {
      let me = this;
      if (languages_loaded && typeof apertium_languages != 'undefined') {
        me.translate();
        return;
      }
      if (languages_loaded && typeof apertium_languages == 'undefined') {
        setTimeout(me.load_languages, 100);
        return;
      }
      languages_loaded = true;
      try {
        translator_iso = await mnm_fetch_json('./iso.json');
        let d = await mnm_fetch_json('https://apertium.wmflabs.org/list');
        apertium_languages = {};
        (d.responseData || []).forEach(function (pair) {
          if (typeof apertium_languages[pair.sourceLanguage] == 'undefined') apertium_languages[pair.sourceLanguage] = [];
          apertium_languages[pair.sourceLanguage].push(pair.targetLanguage);
        });
        me.translate();
      } catch (e) {
        apertium_languages = {}; // mark as loaded but empty so retries don't block
      }
    },
    translate: async function () {
      let me = this;
      if (typeof me.text == 'undefined') return;
      if (typeof me.from == 'undefined') return;
      if (typeof me.to == 'undefined') return;
      if (me.text == '' || me.from == '' || me.to == '' || me.from == me.to) return;
      let from = me.iso(me.from);
      let to = me.iso(me.to);
      if (typeof apertium_languages[from] == 'undefined') {
        //me.note = "Cannot translate from " + from ;
        return;
      }
      if (apertium_languages[from].indexOf(to) == -1) {
        //me.note = "Cannot translate from " + from + ' to ' + to + ', only to ' + apertium_languages[from].join(',');
        return;
      }

      let text = me.filteredText();

      if (typeof translation_cache[from] == 'undefined') translation_cache[from] = [];
      if (typeof translation_cache[from][to] == 'undefined') translation_cache[from][to] = {};

      let d = await mnm_fetch_json('//apertium.wmflabs.org/translate', { langpair: from + '|' + to, q: text });
      me.translation = d.responseData.translatedText;
      //translation_cache[from][to][text] = me.translation;
      me.translated = true;
    },
    filteredText: function () {
      let me = this;
      return me.text.replace(/;/g, '.').replace(/[«»]/g, ' ').replace(/ +/g, ' ');
    },
    iso: function (lang) {
      if (typeof translator_iso != 'undefined' && typeof translator_iso[lang] != 'undefined') return translator_iso[lang];
      return lang;
    }
  },
  template: `<div>
	<span v-if='translated' style='color:#FF9C42'>
		{{translation}}
	</span>
	<span v-else-if='note!=""'>
		<i>{{note}}</i>
	</span>
</div>`
};
