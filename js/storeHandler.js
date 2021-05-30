const util = require('./util.js')
module.exports =  class StoreHandler{
	constructor(kvstore, viewHandler){
		this.kvstore = kvstore;
		this.viewHandler = viewHandler;
		this.delayed_reqs = [];  //store requests that are not yet deliverable (waiting for requests that happened before )
		this.cur_VC = {};  //point-wise maximum of of all delivered VCs, decides if a new request is deliverable
		this.client_count = 0;  //number of client
	}

	handleGetAll(sendRes){
		let resJSON = {}
		console.log("GET all kv pairs");
		resJSON['statusCode'] = 200
		resJSON['body'] = {kvstore:this.kvstore, cur_VC:this.cur_VC}
		sendRes(resJSON);
	}

	handleReq(key, value, method, replicated, sendRes, metadata){
		if (deliverable(metadata)){
			this.handleDeliverableReq(key, value, method, replicated, sendRes, metadata)
			let req_delivered=true
			//see if any req in delayed_reqs can be delivered
			while(req_delivered){
				req_delivered=false
				for (const req of delayed_reqs){
					if(this.deliverable(req.metadata)){
						this.handleDeliverableReq(req.key, req.value, req.method, req.replicated, req.sendRes, req.metadata)
						req_delivered=true
					}
				}
			}
		}else{
			this.delayed_reqs.push({key:key, value: value, method: method, replicated:replicated,, sendRes: sendRes, metadata: metadata})
		}
	}

	handleDeliverableReq(key, value, method, sendRes, metadata){
		let resJSON = {}
		if(replicated){
			resJSON = this.handleReplicated(key, value, method, metadata)
		}else if(method =="GET") {
			resJSON = this.handleGet(key, metadata);
		}else if(method =="PUT"){
			resJSON = this.handlePut(key,value, metadata);
		}
		else if (method =='DELETE'){
			resJSON = this.handleDelete(key, metadata);
		}
		sendRes(resJSON);
	}

	handleReplicated(key, value, method, metadata){
		let resJSON= {}
	    if(method =="PUT"){
			this.kvstore[key] = value
			resJSON= {statusCode:200, msg: "PUT"+key+"="+value+" delivered"}
		}
		else if (method =='DELETE'){
			delete this.kvstore[key]
			resJSON= {statusCode:200, msg: "DELETE"+key+" delivered"}
		}
		else{
			return {statusCode:400, msg: method+" not supported for replicated message"}
		}
		this.cur_VC[client_name] = Math.max(this.cur_VC[client_name], VC[client_name]);
		return resJSON
	}



 	handleGet(key, metadata){
		let resJSON = {}
		console.log("GET key="+key);
		if(key in this.kvstore){
			resJSON['statusCode'] = 200
			resJSON['body'] = {doesExist: true, message: "Retrieved successfully", value: this.kvstore[key] }
			if (!metadata)
				metadata = new_client_metadata()
			metadata['VC'] = util.pntwiseMax(metadata['VC'], this.cur_VC)
		}
		else {
			resJSON['statusCode'] = 404
			resJSON['body'] = {doesExist: false, message: "Error in GET", error: "Key does not exist" }
		}
		resJSON['causal-metadata'] = metadata
		return resJSON
	}

	handlePut(key, value, metadata){
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
			if(key in this.kvstore){
				this.kvstore[key] = value
				resJSON['statusCode'] = 200
				resJSON['body'] = {message: "Updated successfully", replaced: true}
			}
			else {
				this.kvstore[key] = value
				resJSON['statusCode'] = 201
				resJSON['body'] = {message: "Added successfully", replaced: false}
			}

			if (!metadata)
				metadata = new_client_metadata()
			this.viewHandler.broadcast("key-value-store", "PUT", {key:key, value:value, 'causal-metadata':metadata, replicated: true}, (response) => {
			    console.log("replicate PUT to"+url+" succeded")
			})
			let {client_name , VC} = metadata
			this.cur_VC[client_name] = Math.max(this.cur_VC[client_name], VC[client_name]);
			VC[client_name]++;  //increment VC for the client's next response
		}
		resJSON['causal-metadata'] = metadata
		return resJSON
	}

	handleDelete(key, metadata){
		let resJSON = {}
		console.log("DELETE key="+key);
		if(key in this.kvstore){
			resJSON['statusCode'] = 200
			resJSON['body'] = {doesExist: true, message: "Deleted successfully"}
			delete this.kvstore[key]

			if (!metadata)
				metadata = new_client_metadata()
			this.viewHandler.broadcast("key-value-store", "DELETE", {key:key, value:value, 'causal-metadata':metadata, replicated: true}, (response) => {
			    console.log("replicate DELETE to"+url+" succeded")
			})
			let {client_name , VC} = metadata
			this.cur_VC[client_name] = Math.max(this.cur_VC[client_name], VC[client_name]);
			VC[client_name]++;  //increment VC for the client's next response
		}
		else {
			resJSON['statusCode'] = 404
			resJSON['body'] = {doesExist: false, message: "Error in DELETE", error: "Key does not exist"}
		}
		resJSON['causal-metadata'] = metadata
		return resJSON
	}

	deliverable(metadata){
		if(!metadata) //new client
			return true;
		let {client_name , VC} = metadata
		for (const key in VC){
			if (key == client_name && VC[key]==1)
				continue;
			if(!this.cur_VC[key])
				return false
			if (key == client_name && VC[key] != this.cur_VC[key]+1)
				return false
			if (req_VC[key]>this.cur_VC[key])
				return false
		}
		return true
	}


	new_client_metadata(){
		let client_name = this.viewHandler.socket_address+ "_"+ this.client_count.toString()()
		let VC = {}
		VC[client_name] = 1;
		this.client_count++;
		return {client_name: client_name, VC: VC}
	}

};