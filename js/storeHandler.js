const util = require('./util.js')

module.exports =  class StoreHandler{


	constructor(kvstore, viewHandler, index){
		this.kvstore = kvstore;
		this.viewHandler = viewHandler
		this.delayed_reqs = []  //store requests that are not yet deliverable (waiting for requests that happened before )
		this.cur_VC = []  //point-wise maximum of of all delivered VCs, decides if a new request is deliverable
		var len = this.viewHandler.view.length
		for (var i =0; i<len; i++){
		    this.cur_VC.push(0)
		}
		this.index = index //this is the index this replica is represented in the VC
	}	


	async handleReq(key, dataJSON, method, sendRes){
		let metadata = dataJSON['causal-metadata']
		let resJSON = {}
		if(!this.deliverable(metadata) && util.partiallyGreater(metadata['VC'], this.cur_VC))
			await this.gossiping() //get update-to-date kv pairs from other replicas
		if(this.deliverable(metadata)){
			if('broadcasted' in dataJSON && dataJSON['broadcasted']){
			    console.log('broadcasting...')
				resJSON = this.handleBroadcastedReq(key, dataJSON, method)
			}
			else if(method =="GET") {
				resJSON = this.handleGet(key, dataJSON);
			}else if(method =="PUT"){
				resJSON = this.handlePut(key,dataJSON);
			}	
			else if (method =='DELETE'){
				resJSON = this.handleDelete(key,dataJSON);
			}
			sendRes(resJSON);
			/*
			if(method !="GET"){
				this.checkDelayedReqs();
			}*/
		}else{
			/*
			if('broadcasted' in dataJSON && dataJSON['broadcasted']){
				this.delayed_reqs.push({key:key, dataJSON:dataJSON, method:method, sendRes:sendRes})*/
			//else{
			resJSON['statusCode'] = 503
			resJSON['body'] = {message: "metadata error", error: "message currently not deliverable", 'causal-metadata': metadata,
							'currVC':this.cur_VC , 'index': this.index}
			sendRes(resJSON);
			//}
		}

	}

	async gossiping(){
		console.log("gossiping to get up-to-date kv pairs")
		await this.viewHandler.sequentialBroadcast("key-value-store-all", "GET", {}, (response) => {
			if(response.status==200){
				let {kvstore, cur_VC} = response.data
				console.log("gossiping succeeded, replica VC=", cur_VC)
				if(util.partiallyGreater(cur_VC, this.cur_VC)){
				  for (let [key, entry] of Object.entries(kvstore)){
					let VC = entry['VC']
					if (util.partiallyGreater(VC, this.cur_VC)){
					  this.kvstore[key] = entry
					  console.log("in gossip, key",key, "updated to",entry)
					}
				  }
				}
				this.cur_VC = util.pntwiseMax( this.cur_VC , cur_VC);
			}else{
			  console.log("gossiping error, response=", response.data )
			}
		})
	}
	/*
	checkDelayedReqs(){
		var cont = true
		var i = 0
		while (cont){
			cont = false;
			for (let {key, dataJSON, method, sendRes} in this.delayed_reqs){
				let metadata = dataJSON['causal-metadata']
				if (deliverable(metadata)){
					let resJSON = handleBroadcastedReq(key, dataJSON, method)
					sendRes(resJSON)
					cont = true;
				}
			}
		}
	}*/

	handleBroadcastedReq(key, dataJSON, method){
		let {VC, index} = dataJSON['causal-metadata']
		let resJSON = {}
		if(method=="PUT"){
			let value = dataJSON['value']
			if(key in this.kvstore){
				resJSON['statusCode'] = 200
				resJSON['body'] = {message: "Updated successfully", replaced: true}
			}
			else {
				resJSON['statusCode'] = 201
				resJSON['body'] = {message: "Added successfully", replaced: false}
			}
			this.cur_VC[index]++ 
			console.log("updated cur_VC",this.cur_VC)
			this.kvstore[key] = {value:value, index:index, VC: VC}
		}
		if (method == "DELETE"){
            resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Deleted successfully"}
            delete this.kvstore[key]
            this.cur_VC[index]++
            console.log("updated cur_VC",this.cur_VC)
		}
		return resJSON
	}

	handleGetAll(sendRes){
		let resJSON = {}
		console.log("GET all kv pairs");
		resJSON['statusCode'] = 200
		resJSON['body'] = {kvstore:this.kvstore, cur_VC:this.cur_VC}
		sendRes(resJSON);
	}



 	handleGet(key, dataJSON){
		let resJSON = {}
		let metadata = dataJSON['causal-metadata']
		console.log("GET key="+key);
		if(key in this.kvstore){
			let {value, index, VC} = this.kvstore[key]
			if (!metadata)
				metadata = {VC: VC, index: index}
			else
				metadata = {VC: this.pointwiseMax(VC, metadata['VC']), index: index}
			resJSON['statusCode'] = 200
			resJSON['body'] = {doesExist: true, message: "Retrieved successfully", value: value,
								'causal-metadata': metadata }
		}
		else {
			resJSON['statusCode'] = 404
			resJSON['body'] = {doesExist: false, message: "Error in GET", error: "Key does not exist",
								'causal-metadata': metadata }
		}

		return resJSON
	}

	handlePut(key, dataJSON){
		let value = dataJSON['value'];
		let metadata = dataJSON['causal-metadata']
		let resJSON = {}
		console.log("PUT key="+key+", value="+value)
		if (value===undefined){
			resJSON['statusCode'] = 400
			resJSON['body'] = {message: "Error in PUT", error: "Value is missing" }
		}
		else if (key.length>50){
			resJSON['statusCode'] = 400
			resJSON['body'] = {message: "Error in PUT", error: "Key is too long" }
		}
        else {
			if (!metadata){
			    let vc = []
            	var len = this.cur_VC.length
                for (var i =0; i<len; i++){
                    vc.push(0)
                }
		        metadata = {VC:vc, index:this.index}
		    }
			metadata['index'] = this.index
            if(key in this.kvstore){
                resJSON['statusCode'] = 200
                resJSON['body'] = {message: "Updated successfully", replaced: true}
            }
            else {
                resJSON['statusCode'] = 201
                resJSON['body'] = {message: "Added successfully", replaced: false}
            }
			this.viewHandler.broadcast("key-value-store/"+key, "PUT", { 'causal-metadata': metadata, broadcasted:true, value:value }, (response) => {
				console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
			})
			this.cur_VC[this.index] ++
			metadata['VC'][this.index]= this.cur_VC[this.index]
			this.kvstore[key] = {value:value, index: this.index, VC: this.cur_VC}
			resJSON['body']['causal-metadata'] = metadata
        }

		return resJSON
	}

	handleDelete(key, dataJSON){
		let metadata = dataJSON['causal-metadata']
		let resJSON = {}
		console.log("DELETE key="+key);
		if(key in this.kvstore == false){
		    resJSON['statusCode'] = 404
            resJSON['body'] = {doesExist: false, message: "Error in DELETE", error: "Key does not exist"}
		}
		else {
		    if (!metadata){
		        let vc = []
		        var len = this.cur_VC.length
                for (var i =0; i<len; i++){
                    vc.push(0)
                }
                metadata = {VC:vc, index:this.index}
            }
            metadata['index'] = this.index

			resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Deleted successfully"}
            delete this.kvstore[key]

            this.viewHandler.broadcast("key-value-store/"+key, "DELETE", { 'causal-metadata': metadata, broadcasted:true}, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })
            this.cur_VC[this.index] ++
            metadata['VC'][this.index]= this.cur_VC[this.index]
            resJSON['body']['causal-metadata'] = metadata
		}
		return resJSON
	}


	deliverable (metadata){
		if (!metadata)
			return true
		let {VC, index} = metadata
		console.log("VC: ", VC, "cur_VC: ", this.cur_VC, 'index: ', index)
		if (VC.length>this.cur_VC.length)
			return false
		for (let i =0;i<this.cur_VC.length;i++){
			if (i == index && VC[i] != this.cur_VC[i]){
				return false
			}else if (i!=index && VC[i]>this.cur_VC[i]){
				return false
			}
		}
		return true
	}

	pointwiseMax (arrA, arrB){
		console.log("arrA:", arrA, "arrB:", arrB)
		if(!arrA)
			return arrB
		if(!arrB)
			return arrA
	    var retArr= []
	    for (var i =0; i<arrA.length; i++){
	        if (arrA[i]>arrB[i]){
	            retArr.push(arrA[i])
	        }
	        else{
	            retArr.push(arrB[i])
	        }
	    }
	    return retArr
	}

    /*broadcast(m, key, value, metadata){
        let replicas = this.viewHandler.view
        //for (var i =0; i<replicas.length; i++){
            let resJSON = {}
            var address = replicas[1].split(':')
            const options = {
                hostname: address[0],
                port: address[1],
                path: '/key-value-store/'+key,
                method: m,
                headers: {
                  'Content-Type': 'application/json',
                }
            }

            console.log("Forwarding "+m+ " to "+replicas[1]+", method="+m+", key="+key+", value="+value)
            const http = require('http');
            const req = http.request(options, res => {
                let data = ''
                res.on('data', (chunk) => {
                    data+=chunk;
                })
                res.on('end', () => {
                    resJSON['statusCode'] = res.statusCode
                    resJSON['body'] = JSON.parse(data)
                    resJSON.update({'broadcasted':true})
                    sendRes(resJSON)
                })
            })

            req.on('error', error => {
                resJSON['statusCode'] = 503
                resJSON['body'] = {error: "Replica is down", message: 'Error in '+ m}
                sendRes(resJSON)
            })

            if(value){
                const reqJSON = {value: value};
                req.write(JSON.stringify(reqJSON));
            }
            req.end()

            //if (i+1 == this.index){
            //    i++;
            //}
        //}
    }*/

};