const util = require("./util.js");
const axios = require('axios');
const stringHash = require('string-hash');

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
		        resJSON = await this.handlePutReshard(data);
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
        //console.log("view: "+ this.viewHandler.view)
        var tempView =[]
        for (var j=0; j<this.viewHandler.view.length; j++){
            if (this.viewHandler.view[j] != this.viewHandler.socket_address)
                tempView.push(this.viewHandler.view[j])
        }
        //console.log("shardcount:"+ this.shard_count+" shard_id:"+shard_id)
        var members = await this.handleGetIdMembersHelper(tempView, shard_id);
        //console.log("members: "+ members);
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
                    //console.log("here is temp: "+res.data['socket-address']);
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
        let res = await this.handleGetIdMembers(shard_id);
        var nodesInShard = res['body']['shard-id-members'];
        var count = 0;
        if (this.myShard == shard_id){
            count += Object.keys(this.storeHandler.kvstore).length;
            if (count == 0){
                for (var i =0; i<nodesInShard.length; i++){
                    if (nodesInShard[i]!=this.viewHandler.socket_address){
                        await axios.get("http://"+nodesInShard[i]+"/"+"key-value-store-all").then(res=>{
                            var allKeys = res.data['kvstore']
                            count+= Object.keys(allKeys).length
                            console.log("adding to count: "+count)
                        })
                    }
                    if (count>0)
                        break;
                }
            }
        }
        else{
            await axios.get("http://"+nodesInShard[0]+"/"+"key-value-store-all").then(res=>{
                var allKeys = res.data['kvstore']
                count+= Object.keys(allKeys).length
                console.log("adding to count: "+count)
            })
        }
        resJSON['body'] = {message:"Key count of shard ID retrieved successfully", "shard-id-key-count":count}
        return resJSON
    }

	async handlePutMember(shard_id, socket_address){
		let resJSON = {}
        console.log("PUT member in shard");
        await axios.put("http://"+socket_address+"/key-value-store-shard/set-shard/"+shard_id, {}).then(res=>{
            console.log(res.data.message);
        });
        resJSON['statusCode'] = 200
        return resJSON
	}

	async handlePutReshard(shard_count){
		let resJSON = {}
        console.log("PUT reshard");
        if (this.viewHandler.view.length/shard_count < 2){
            resJSON['statusCode'] = 400
            resJSON['body'] = {message:"Not enough nodes to provide fault-tolerance with the given shard count!"}
            return resJSON;
        }
        else if (shard_count== this.shard_count){
            resJSON['statusCode'] = 200
            resJSON['body'] = {message:"Resharding done successfully"}
            return resJSON;
        }
        else{
            this.handlePutShardCount(shard_count);
            await this.viewHandler.broadcastSequential("key-value-store-shard/set-shard-count/"+shard_count, "PUT", {}, (res) => {
                console.log("broadcast of changing shard counts successful");
            });
            console.log("after first broadcast")
            /*for (var i = 0; i< this.viewHandler.view.length; i++){
                let address = this.viewHandler.view[i];
                let kvstore;
                console.log("before get all")
                await axios.get("http://"+address+"/"+"key-value-store-all").then(res=>{
                    kvstore = res.data['kvstore']
                });
                console.log("got key value struct from node")
                let keys = Object.keys(kvstore);
                for (var k =0; k<keys.length; k++){
                    let key = keys[k]
                    console.log("key "+key+ " value "+ kvstore[key]['value']);

                    await axios.delete("http://"+address+"/"+"key-value-store/"+key).then(res=>{
                        console.log("reshard: deleted key in shard")
                    });

                    //let resTemp = await this.handleGetIdMembers(this.keyToShardID(key));
                    //let newShard = resTemp['body']['shard-id-members']
                    await axios.put("http://"+address+"/"+"key-value-store/"+key, {'value':kvstore[key]['value'], 'causal-metadata':""}).then(res=>{
                        console.log("reshard: added back to correct shard")
                    });

                    //recalculate
                    //delete
                    //put
                }

            }*/

            //iterate through all keys and recalculate what shards they belong to
            /*for (var i =0; i<shard_count; i++){
                let keysInShard = []
                if (this.myShard == i){
                    keysInShard.push(this.storeHandler.kvstore);
                }
                //broadcast in shard i get all keys
                let res = await this.handleGetIdMembers(i);
                let shard = res['body']['shard-id-members'];
                await this.viewHandler.broadcastUntilSuccess(shard, "key-value-store-all", "GET", {}, (res) => {
                    keysInShard.push(res.data['kvstore'])
                });
                //for each key, recalcuate hash and see if it has to move
                for (var j =0; j<keysInShard.length; j++){
                    let keys = Object.keys(keysInShard[j]);
                    for (var k =0; k<keys.length; k++){
                        let key = keys[k]
                        console.log("key "+key+ " value "+ keysInShard[j][key]['value']);
                        if (this.keyToShardID(key)!=i){
                            await this.viewHandler.broadcastUntilSuccess(shard, "key-value-store/"+key, "DELETE",{},(res)=>{
                                console.log("reshard: deleted key in shard")
                            })

                            let resTemp = await this.handleGetIdMembers(this.keyToShardID(key));
                            let newShard = resTemp['body']['shard-id-members']
                            await this.viewHandler.broadcastUntilSuccess(newShard, "key-value-store/"+key, "PUT", {'value':0, 'causal-metadata':""},
                            (res)=>{
                                console.log("reshard: added back to correct shard")
                            })
                        }
                        //recalculate
                        //delete
                        //put
                    }
                }
            }*/

            resJSON['statusCode'] = 200
            resJSON['body'] = {message:"Resharding done successfully"}
        }
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

	handlePutShardCount(shard_count){ //recalculate the higher variables after shard_count changed
	    let resJSON = {}
        console.log("PUT shard count");
        this.shard_count = shard_count;
        this.shardIds = [];
        for (var i =0; i< shard_count; i++){
            this.shardIds.push(i);
        }
        var j;
        for (j=0; j<this.viewHandler.view.length; j++){
            if (this.viewHandler.socket_address == this.viewHandler.view[j])
                break
        }
        this.myShard = j % shard_count;
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node put successfully"}
        return resJSON
	}

	keyToShardID(key){
        return stringHash(key) % this.shard_count;
	}

	inThisShard(key){
	    return this.myShard == this.keyToShardID(key);
	}

}