const Util ={
	//a, b should be arrays representing vector clocks
    pntwiseMax: function (a, b){
		var l = a.length>b.length? a.length: b.length
		let res = [];
		for (let i =0;i<l;i++){
			if (i>= a.length){
				res.push(b[i])
			}else if(i>=b.length){
				res.push(a[i])
			}else{
				res.push(Math.max(a[i], b[i]))
			}

		}
		return res
	},

	//check if a is greater than b in at least one index
	partiallyGreater: function (a, b){
		if (a.length>b.length)
			return true
		for(let i =0; i<b.length;i++){
			if (a[i]>b[i])
				return true
		}
		return true
	}
}
module.exports = Util;


