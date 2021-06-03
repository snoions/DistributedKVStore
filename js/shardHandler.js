const util = require("./util.js")
const axios = require('axios');
module.exports =  class ShardHandler{
    constructor(shard_count, view, store){
        this.shard_count = shard_count;
        this.viewHandler = view;
        //this.view = store.viewHandler.view;//view;
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
        this.myShard = -1;
        if (shard_count!=-1)
            this.myShard = j % shard_count;
	}

    async handleReq(func, shard_id, method, data, sendRes){
		let resJSON = {}
		if(method =="GET") {
		    if (func == "shard-ids")
			    resJSON = this.handleGetIds();
			else if (func == "node-shard-id")
			    resJSON = this.handleGetNodeShardId();
			else if (func == "shard-id-members")
			    resJSON = await this.handleGetIdMembers(shard_id);
			else if (func == "shard-id-key-count")
			    resJSON = this.handleGetShardKeyCount(shard_id);
		}else if(method =="PUT"){
		    if (func == "add-member")
		        resJSON = this.handlePutMember(shard_id, data);
		    else if (func == "reshard")
		        resJSON = this.handlePutReshard(shard_id);
		}
		console.log("handled the request")
		sendRes(resJSON);
	}

    handleGetIds(){
		let resJSON = {}
		console.log("GET shard id");
		resJSON['statusCode'] = 200
		resJSON['body'] = {message:"Shard IDs retrieved successfully", "shard-ids": this.shardIds}
		return resJSON
	}

	handleGetNodeShardId(){
	    let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node retrieved successfully", "shard-id": this.myShard,
                            "socket-address":this.viewHandler.socket_address}
        return resJSON
	}

    async handleGetIdMembers(shard_id){
        let resJSON = {}
        console.log("GET members in a shard id");
        console.log("view: "+ this.viewHandler.view)
        var tempView =[]
        for (var j=0; j<this.viewHandler.view.length; j++){
            if (this.viewHandler.view[j] != this.viewHandler.socket_address)
                tempView.push(this.viewHandler.view[j])
        }
        console.log("shardcount:"+ this.shard_count+" shard_id:"+shard_id)
        var members = await this.handleGetIdMembersHelper(tempView, shard_id);
        console.log("members: "+ members);
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Members of shard ID retrieved successfully", "shard-id-members": members}
        return resJSON
    }

    async handleGetIdMembersHelper(view, shard_id){
        var members = []
        if (this.myShard == shard_id){
            members.push(this.viewHandler.socket_address)
        }
        await Promise.all(view.map(obj =>
            axios.get("http://"+obj+"/key-value-store-shard/node-shard-id").then(res=>{
                if (res.data['shard-id'] == shard_id){
                    console.log("here is temp: "+res.data['socket-address']);
                    members.push(res.data['socket-address']);
                }
            })
        ));
        return members;
    }

    handleGetShardKeyCount(shard_id){
        let resJSON = {}
        console.log("GET number of keys in a shard");
        resJSON['statusCode'] = 200
        var count = Object.keys(this.storeHandler.kvstore).length;
        var nodesInShard = handleGetIdMembers(shard_id)['body'];
        for (var i =0; i<nodesInShard; i++){
            if (nodesInShard[i]!= this.viewHandler.socket_address){
                var url = "http://"+nodesInShard[i]+"/"+"key-value-store-all";
                axios.get(url).then(res=>{
                    var allKeys = res.data['kvstore']
                    count+= Object.keys(allKeys).length
                });
            }
        }
        resJSON['body'] = {message:"Key count of shard ID retrieved successfully", "shard-id-key-count":count}
        return resJSON
    }

	handlePutMember(shard_id, socket_address){
	    //send a request to the node at socket address
	    //set myshard
	    //set the shard count and shard ids -broadcast

		let resJSON = {}
        console.log("PUT member in shard");
        resJSON['statusCode'] = 200
        return resJSON
	}

	handlePutReshard(shard_id){
		let resJSON = {}
        console.log("PUT reshard");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Resharding done successfully"}
        return resJSON
	}

	handlePutNodeShardId(shard_id){
	    let resJSON = {}
        console.log("PUT node shard id");
        this.myShard = shard_id
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node put successfully"}
        return resJSON
	}

	handlePutShardCount(shard_count){
	    let resJSON = {}
        console.log("PUT shard count");
        this.shard_count = shard_count
        for (var i =0; i< shard_count; i++){
            this.shardIds.push(i);
        }
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node put successfully"}
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