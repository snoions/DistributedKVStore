const Util ={
	//a, b should be arrays representing vector clocks
	/* array version
    pntwiseMax: function (a, b){
		var s, l //longer and shorter arrays
		if(a.length>b.length){
			s = b;
			l = a;
		}else{
			s = a;
			l = b;
		}
		let res = [];
		for (let i =0;i<s.length;i++){
			res[i] = s[i]>l[i]? s[i]: l[i];
		}
		for(let i=s.length; i<l.length;i++){
			res[i] = l[i]
		}
		return res
	}*/
	pntwiseMax: function (VC1, VC2){
		let VC = {}
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
	}
}

module.exports = Util;

