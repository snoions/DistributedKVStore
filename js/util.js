const Util ={
	//a, b should be arrays representing vector clocks
	pntwiseMax: function (VC1, VC2){
		let VC = {}
		if (!(VC2) ) VC2 = {}
		for (const key in VC1){
			if (!(key in VC2))
				VC[key] = VC1[key]
			else
				VC[key] = Math.max(VC1[key], VC2[key])
		}
		for (const key in VC2){
			if (!(key in VC))
				VC[key] =  VC2[key]
		}
		return VC
	},

}
module.exports = Util;