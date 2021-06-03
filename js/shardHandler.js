const util = require("./util.js")
const axios = require('axios');
module.exports =  class ShardHandler{
    constructor(shard_count, view, store){
        this.shard_count = shard_count;
        this.viewHandler = view;
        //this.view = store.viewHandler.view;//view;
        this.storeHandler = store;
        //this.shardDict = {};
        this.shardIds = []; //[0,..,shard_count-1]
        for (var i =0; i< shard_count; i++){
            this.shardIds.push(i);
            //this.shardDict[i] = [];
        }
        var pos;
        for (var j=0; j<this.viewHandler.view.length; j++){
            if (this.viewHandler.socket_address == this.viewHandler.view[j])
                pos = j;
            //this.shardDict[j % shard_count].push(this.viewHandler.view[j])
        }
        this.myShard = -1;
        if (shard_count!=-1)
            this.myShard = pos % shard_count;
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
			    resJSON = await this.handleGetShardKeyCount(shard_id);
		}else if(method =="PUT"){
		    if (func == "add-member")
		        resJSON = await this.handlePutMember(shard_id, data);
		    else if (func == "reshard")
		        resJSON = this.handlePutReshard(shard_id);
		    else if (func == "set-shard")
		        resJSON = this.handlePutNodeShardId(shard_id);
		    else if (func == "set-shard-count")
		        resJSON = this.handlePutShardCount(shard_id);
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
        //this.shardDict[shard_id] = members;
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

    async handleGetShardKeyCount(shard_id){
        let resJSON = {}
        console.log("GET number of keys in a shard");
        resJSON['statusCode'] = 200
        var nodesInShard = await handleGetIdMembers(shard_id)['body'];
        var tempView =[]
        for (var i =0; i<nodesInShard; i++){
            if (nodesInShard[i]!= this.viewHandler.socket_address)
                tempView.push(nodesInShard[i])
        }
        var count = await this.handleGetShardKeyCountHelper(shard_id, tempView);
        resJSON['body'] = {message:"Key count of shard ID retrieved successfully", "shard-id-key-count":count}
        return resJSON
    }

    async handleGetShardKeyCountHelper(shard_id, nodesInShard){
        var count = 0;
        if (this.myShard == shard_id){
            count += Object.keys(this.storeHandler.kvstore).length;
        }
        await Promise.all(nodesInShard.map(obj =>
            axios.get("http://"+obj+"/"+"key-value-store-all").then(res=>{
                var allKeys = res.data['kvstore']
                count+= Object.keys(allKeys).length
            })
        ));
        return count;
    }

	async handlePutMember(shard_id, socket_address){
	    //send a request to the node at socket address
	    //set myshard
	    //set the shard count and shard ids -broadcast
		let resJSON = {}
        console.log("PUT member in shard");
        await axios.put("http://"+socket_address+"/key-value-store-shard/set-shard/"+shard_id, {}).then(res=>{
            console.log(res.data.message);
        })
        if (shard_id > (this.shard_count-1)){
            this.handlePutShardCount(shard_id);
            this.viewHandler.broadcast("key-value-store-shard/set-shard-count/"+shard_id, "PUT", {}, (res) => {
                console.log("broadcast of adding new member successful");
            })
        }
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
        this.shard_count = shard_count+1;
        this.shardIds = [];
        for (var i =0; i<= shard_count; i++){
            this.shardIds.push(i);
        }
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node put successfully"}
        return resJSON
	}

	keyToShardID(key){
        if (key[0] >= 'a' && key[0] < 'n')
			return 0
		else
			return 1
	}

	inThisShard(key){
	    return this.myShard == this.keyToShardID(key);
	}

	async broadcastInThisShard(endpoint, method, data, thenFunc){
	    let res = await this.handleGetIdMembers(this.myShard);
	    let shardMembers = res['body']['shard-id-members']
        const shard_others = shardMembers.filter(address => address!=this.socket_address );  //other replicas in the shard
        console.log("in broadcastInThisShard, shard_others=", shard_others)
        for (let address of shard_others) {
            this.viewHandler.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
         }
    }

    //same as broadcast but sends are executed one by one
    async sequentialBroadcast(endpoint, method, data, thenFunc){
        let res = await this.handleGetIdMembers(this.myShard);
        let shardMembers = res['body']['shard-id-members']
        const shard_others = shardMembers.filter(address => address!=this.socket_address );  //other replicas in the shard
        console.log("in broadcast, shard_others=", shard_others)
        for (let address of shard_others) {
            await this.viewHandler.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
         }
    }

    //broadcast to each address in the shard until success
    async broadcastUntilSuccess(shard, endpoint, method, data, thenFunc){ // look into what exactly shard is
        const shard_others = shard.filter(address => address!=this.socket_address );  //other replicas in the shard
        console.log("in BroadcastUntilSuccess, shard_others=", shard_others)
        let cont = true
        for (let address of shard_others) {
            if (!cont)
               break;
            await this.viewHandler.sendAndDetectCrash(address, endpoint, method, data, (response)=>{thenFunc(response);console.log("stop broadcast");cont=false})
         }
    }
}