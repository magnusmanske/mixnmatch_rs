export default {
	name: 'timestamp',
	props: ['ts'],
	template: `<span v-if='typeof ts != "undefined" && ts!=null'>
		{{ts.substr(0,4)+"-"+ts.substr(4,2)+"-"+ts.substr(6,2)+"\\u00a0"+ts.substr(8,2)+":"+ts.substr(10,2)+":"+ts.substr(12,2)}}
	</span>`
};
