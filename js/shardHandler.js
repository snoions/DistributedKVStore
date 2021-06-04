const util = require("./util.js");
const axios = require('axios');
const stringHash = require('string-hash');

module.exports =  class ShardHandler{
    constructor(shard_count, view, store, messenger){
        this.shard_count = shard_count;
        this.viewHandler = view;
        //this.view = store.viewHandler.view;//view;
        this.storeHandler = store;
        //this.shardDict = {};
        
        this.shardDict = []
        this.viewHandler.view.forEach((address, index )=> {
            let shardID = index % shard_count
            if (!this.shardDict[shardID]) this.shardDict[shardID]=[]
            this.shardDict[shardID].push(address)
            if (address==this.viewHandler.socket_address) this.myShardID= shardID
        })
        console.log("shardDict:", this.shardDict)

        if (this.myShardID == undefined)
            throw new Error("socket address", socket_address, " not in shard", shard); 
        this.myShard = this.shardDict[this.myShardID]
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
		}
		console.log("handled the request")
		sendRes(resJSON);
	}

    handleGetIds(){
		let resJSON = {}
		console.log("GET shard id");
		resJSON['statusCode'] = 200
		resJSON['body'] = {message:"Shard IDs retrieved successfully", "shard-ids": Array.from(this.shardDict, (_, i) => i)}
		return resJSON
	}

	handleGetNodeShardId(){
	    let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node retrieved successfully", "shard-id": this.myShardID,
                            "socket-address":this.viewHandler.socket_address}
        return resJSON
	}

    async handleGetIdMembers(shard_id){
        let resJSON = {}
        console.log("GET members in a shard id");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Members of shard ID retrieved successfully", "shard-id-members": this.shardDict[shard_id]}
        return resJSON
    }


    async handleGetShardKeyCount(shard_id){
        let resJSON = {}
        console.log("GET number of keys in a shard");
        resJSON['statusCode'] = 200
        var count = 0;
        if (this.myShardID == shard_id){
            await this.storeHandler.gossiping()
            count = Object.keys(this.storeHandler.kvstore).length
        }
        else{
            await axios.get("http://"+shardDict[shard_id][0]+"/"+"key-value-store-all").then(res=>{
                var allKeys = res.data['kvstore']
                count = Object.keys(allKeys).length
            })
        }
        resJSON['body'] = {message:"Key count of shard ID retrieved successfully", "shard-id-key-count":count}
        return resJSON
    }

	async handlePutMember(shard_id, data){
		let resJSON = {}
        console.log("PUT member in shard");
        this.shardDict[shard_id].push(data['socket_address'])
        if (!(data['broadcasted'])){
            data = {...data, broadcasted:true}
            await this.messenger.broadcast(this.viewHandler.view, "/key-value-store-shard/set-shard/"+shard_id, "PUT",data,  (res)=>{
                console.log(res.data.message);
            });
        }
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

	keyToShardID(key){
        return stringHash(key) % this.shard_count;
	}

	inThisShard(key){
	    return this.myShard == this.keyToShardID(key);
	}

}