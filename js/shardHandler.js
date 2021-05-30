const util = require("./util.js")
const axios = require('axios');
module.exports =  class ShardHandler{
    constructor(shard_count, view, store){
        this.shard_count = shard_count;
        this.viewHandler = view;
        this.storeHandler = store;
        this.shardIds = []; //[0,..,shard_count-1]
        for (var i =0; i< shard_count; i++){
            this.shardIds.push(i);
        }
        var j;
        for (j=0; j<this.viewHandler.view.length; j++){
            if (this.viewHandler.socket_address == this.viewHandler.view[j])
                break;
        }
        this.myShard = j % shard_count;
	}

    handleReq(func, shard_id, method, sendRes){
		let resJSON = {}
		if(method =="GET") {
		    if (func == "shard-ids")
			    resJSON = this.handleGetIds();
			else if (func == "node-shard-id")
			    resJSON = this.handleGetNodeShardId();
			else if (func == "shard-id-members")
			    resJSON = this.handleGetIdMembers(shard_id);
			else if (func == "shard-id-key-count")
			    resJSON = this.handleGetShardKeyCount(shard_id);
		}else if(method =="PUT"){
		    if (func == "add-member")
		        resJSON = this.handlePutMember(shard_id);
		    else if (func == "reshard")
		        resJSON = this.handlePutReshard(shard_id);
		}
		sendRes(resJSON);
	}

    handleGetIds(){
		let resJSON = {}
		console.log("GET shard id");
		resJSON['statusCode'] = 200
		resJSON['body'] = {message:"Shard IDs retrieved successfully", shard-ids: this.shardIds}
		return resJSON
	}

	handleGetNodeShardId(){
	    let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200

        resJSON['body'] = {message:"Shard ID of the node retrieved successfully", shard-id: this.myShard}
        return resJSON
	}

    handleGetIdMembers(shard_id){
        let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        var members = []
        for (var j=0; j<this.viewHandler.view.length; j++){
            if (j %  this.shard_count == shard_id)
                members.push(this.viewHandler.view[j])
        }
        resJSON['body'] = {message:"Members of shard ID retrieved successfully", shard-id-members: members}
        return resJSON
    }

    handleGetShardKeyCount(shard_id){
        let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        var count = Object.keys(this.storeHandler.kvstore).length;
        resJSON['body'] = {message:"Key count of shard ID retrieved successfully",shard-id-key-count:count}
        return resJSON
    }

	handlePutMember(shard_id){
		let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        return resJSON
	}

	handlePutReshard(shard_id){
		let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Resharding done successfully"}
        return resJSON
	}

    broadcast(endpoint, method, data, thenFunc){
		const view_other = this.view.filter(address => address!=this.socket_address );  //view excluding the address of the current replica
		console.log("in broadcast, view_other=", view_other)
		for (let address of view_other) {
		    this.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
		 }
	}

	async sequentialBroadcast(endpoint, method, data, thenFunc){
		const view_other = this.view.filter(address => address!=this.socket_address );  //view excluding the address of the current replica
		console.log("in broadcast, view_other=", view_other)
		for (let address of view_other) {
		    await this.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
		 }
	}


	async sendAndDetectCrash(address,endpoint, method, data, thenFunc){
		let count = 1
		let retry = true
		 while(retry){
			if(count>1)
			 	await new Promise(r => setTimeout(r, 500));  //sleep half a second before retry
			retry = false
			let url = "http://"+address+"/"+endpoint
			console.log("sending to "+url+ " attempt ", count)
			await axios({url: url, method: method, data: data})
		    .then (thenFunc)
		    .catch((error) => {
				if (error.response) {
					console.log("Response error", {statusCode: error.response.status, data: error.response.data, url:  url});
				} else if (error.request) {
					// The request was made but no response was received
					console.log("no response from ", address);
					if (count==this.crash_threshold){
						console.log(address+" crashed")
						this.view = this.view.filter(a => a!=address ) 
						this.broadcast('key-value-store-view', 'DELETE', {"socket-address": address})  //broadcast to delete the crashed node
					}else{
						retry = true;
					 	count++;
					}
				}else {
					console.log('axios error', error);
				}
				 
			});
		}
	}

	
}