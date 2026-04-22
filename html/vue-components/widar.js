import { mnm_notify, tt_update_interface, setWidar } from './store.js';

export const UserLink = {
	name: 'userlink',
	props: ['username', 'user_id', 'catalog_id'],
	computed: {
		mnm_link: function () {
			if (this.user_id == null || this.user_id === '') return '';
			var path = '/user/' + this.user_id;
			if (this.catalog_id) path += '/' + this.catalog_id;
			return '#' + path;
		},
		display_name: function () {
			if (this.username === 'automatic') return 'Automatic, preliminary matcher';
			if (this.username === 'Automatic name/date matcher') return 'Automatic name/date matcher';
			if (this.username === 'Auxiliary data matcher') return 'Auxiliary data matcher';
			return this.username || '\u2014';
		},
		is_system: function () {
			return !this.username || /^-?\d+$/.test(this.username) ||
				this.username === 'automatic' || this.username === 'Automatic name/date matcher' || this.username === 'Auxiliary data matcher';
		}
	},
	template: '<span>' +
		'<a v-if="mnm_link" :href="mnm_link">{{display_name}}</a>' +
		'<span v-else-if="is_system">{{display_name}}</span>' +
		'<a v-else :href="\'https://www.wikidata.org/wiki/User:\'+encodeURIComponent(username.replace(/ /g,\'_\'))" target="_blank" class="wikidata">{{username}}</a>' +
		'</span>'
};

var WIDAR_CACHE_KEY = 'mnm_widar_login';

export default {
	name: 'widar',
	data: function () {
		return {
			is_logged_in: false,
			userinfo: {},
			widar_api: './api.php',
			loaded: false,
			is_catalog_admin: false,
			mnm_user_id: 0
		}
	},
	created: function () {
		setWidar(this);
		this.restoreFromCache();
		this.checkLogin();
	},
	updated: function () { tt_update_interface(); },
	mounted: function () { tt_update_interface(); },
	methods: {
		restoreFromCache: function () {
			try {
				var raw = sessionStorage.getItem(WIDAR_CACHE_KEY);
				if (!raw) return;
				var cached = JSON.parse(raw);
				if (cached && cached.is_logged_in && cached.userinfo && cached.userinfo.name) {
					this.is_logged_in = true;
					this.userinfo = cached.userinfo;
					this.is_catalog_admin = !!cached.is_catalog_admin;
					this.mnm_user_id = cached.mnm_user_id | 0;
					this.loaded = true;
				}
			} catch (e) { /* sessionStorage unavailable or corrupt — ignore */ }
		},
		saveToCache: function () {
			try {
				sessionStorage.setItem(WIDAR_CACHE_KEY, JSON.stringify({
					is_logged_in: this.is_logged_in,
					userinfo: this.userinfo,
					is_catalog_admin: this.is_catalog_admin,
					mnm_user_id: this.mnm_user_id
				}));
			} catch (e) { /* quota or private-mode — ignore */ }
		},
		clearCache: function () {
			try { sessionStorage.removeItem(WIDAR_CACHE_KEY); } catch (e) {}
		},
		checkLogin: async function () {
			const me = this;
			try {
				var resp = await fetch(me.widar_api + '?' + new URLSearchParams({ query: 'widar', action: 'get_rights', botmode: 1 }));
				var d = await resp.json();
				me.loaded = true;
				if (d.result && d.result.query && d.result.query.userinfo) {
					me.is_logged_in = true;
					me.userinfo = d.result.query.userinfo;
					try {
						var resp2 = await fetch('./api.php?' + new URLSearchParams({ query: 'get_user_info', username: me.getUserName() }));
						var d2 = await resp2.json();
						me.is_catalog_admin = !!(d2.data && d2.data.is_catalog_admin == 1);
						me.mnm_user_id = (d2.data && d2.data.id) | 0;
					} catch (e) { /* non-critical */ }
					me.saveToCache();
				} else {
					// Server says not logged in — clear any stale cache
					me.is_logged_in = false;
					me.userinfo = {};
					me.is_catalog_admin = false;
					me.mnm_user_id = 0;
					me.clearCache();
				}
			} catch (e) {
				me.loaded = true;
				// Network error — keep cached state if we have it
			}
		},
		run: async function (params, callback, _retries) {
			const me = this;
			const maxRetries = 5;
			_retries = _retries || 0;
			params.tool_hashtag = "mix'n'match";
			params.botmode = 1;
			params.query = 'widar';
			try {
				var resp = await fetch(me.widar_api, {
					method: 'POST',
					headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
					body: new URLSearchParams(params)
				});
				var d = await resp.json();
				if (d.error != 'OK') {
					var retryable = /Invalid token|happen|Problem creating item/.test(d.error) ||
						(params.action != 'create_redirect' && /failed/.test(d.error));
					if (_retries < maxRetries && retryable) {
						console.log("ERROR (re-trying " + (_retries + 1) + "/" + maxRetries + ")", params, d);
						setTimeout(function () { me.run(params, callback, _retries + 1); }, 500 * (_retries + 1));
					} else {
						if (_retries >= maxRetries) mnm_notify('Wikidata edit failed after ' + maxRetries + ' retries: ' + d.error, 'danger', 8000);
						console.log("ERROR (aborting)", params, d);
						callback(d);
					}
				} else {
					callback(d);
				}
			} catch (e) {
				if (_retries < maxRetries) {
					console.log("Network error (re-trying " + (_retries + 1) + "/" + maxRetries + ")", params);
					setTimeout(function () { me.run(params, callback, _retries + 1); }, 1000 * (_retries + 1));
				} else {
					mnm_notify('Network error: could not reach Wikidata after ' + maxRetries + ' retries', 'danger', 8000);
					callback({ error: 'Network error after ' + maxRetries + ' retries' });
				}
			}
		},
		getUserName: function () { return this.userinfo.name; }
	},
	template: `<div style='margin-left:5px;margin-right:5px;text-align:center' v-if="loaded">
		<div v-if='is_logged_in' style='line-height:1.4em'><span tt="welcome"></span><br />
			<userlink :username="userinfo.name" />
		</div>
		<div v-else><i tt="log_into_widar"></i></div>
	</div>`
};
