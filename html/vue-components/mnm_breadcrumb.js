/**
 * Breadcrumb navigation component.
 *
 * Usage:
 *   <mnm-breadcrumb :crumbs="[
 *     {text: 'Catalog Name', to: '/catalog/123'},
 *     {tt: 'common_names'}
 *   ]"></mnm-breadcrumb>
 *
 * The "Home" link is always prepended automatically.
 * Each crumb may have:
 *   - `text` (plain string) OR `tt` (translation key) — exactly one required
 *   - `to` (route path) — if omitted, the crumb is rendered as the active page
 */
export default {
	name: 'mnm-breadcrumb',
	props: {
		crumbs: { type: Array, default: function () { return []; } }
	},
	template: `
	<nav aria-label="breadcrumb">
		<ol class="breadcrumb mb-1" style="font-size:0.85rem">
			<li class="breadcrumb-item"><router-link to="/">Home</router-link></li>
			<li v-for="(c, i) in crumbs" :key="i"
				:class="'breadcrumb-item' + (i === crumbs.length - 1 ? ' active' : '')"
				:aria-current="i === crumbs.length - 1 ? 'page' : null">
				<router-link v-if="c.to" :to="c.to">
					<span v-if="c.tt" :tt="c.tt"></span><span v-else>{{c.text}}</span>
				</router-link>
				<template v-else>
					<span v-if="c.tt" :tt="c.tt"></span><span v-else>{{c.text}}</span>
				</template>
			</li>
		</ol>
	</nav>
`
};
