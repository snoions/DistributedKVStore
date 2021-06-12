module.exports =  class ShardHandler{
    constructor(viewState, storeHandler, messenger){
        this.viewState = viewState;
        this.storeHandler = storeHandler;
        this.messenger = messenger
        
        
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
            else if (func == "view-state")
		        resJSON = await this.handlePutViewState(data);
            else if (func=="key-transfer")
                resJSON = this.handleKeyTransfer(data);
		}
		console.log("handled the request")
		sendRes(resJSON);
	}

    handleGetIds(){
		let resJSON = {}
		console.log("GET shard id");
		resJSON['statusCode'] = 200
        console.log("this.viewState.shard_count", this.viewState.shard_count)
		resJSON['body'] = {message:"Shard IDs retrieved successfully", "shard-ids": Array.from({length:this.viewState.shard_count}, (_, i) => i)}
		return resJSON
	}

	handleGetNodeShardId(){
	    let resJSON = {}
        console.log("GET node shard id");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Shard ID of the node retrieved successfully", "shard-id": this.viewState.myShardID,
                            "socket-address":this.viewState.socket_address}
        return resJSON
	}

    async handleGetIdMembers(shard_id){
        let resJSON = {}
        console.log("GET members in a shard id");
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"Members of shard ID retrieved successfully", "shard-id-members": this.viewState.shardDict[shard_id]}
        return resJSON
    }


    async handleGetShardKeyCount(shard_id){
        let resJSON = {}
        console.log("GET number of keys in a shard");
        resJSON['statusCode'] = 200
        var count = 0;
        if (this.myShardID == shard_id){
            await this.storeHandler.gossip()
            console.log("after gossip1")
            count = Object.keys(this.storeHandler.kvstore).length
        }
        else{
            let shard = this.viewState.shardDict[shard_id]
            console.log("before gossip2")
            await this.messenger.broadcastUntilSuccess(shard, "key-value-store-all", "GET", {"gossip":true}, res=>{
                var allKeys = res.data['kvstore']
                count = Object.keys(allKeys).length
                console.log("after gossip2")
            })
        }
        resJSON['body'] = {message:"Key count of shard ID retrieved successfully", "shard-id-key-count":count}
        return resJSON
    }

	async handlePutMember(shard_id, data){
		let resJSON = {}
        console.log("PUT member in shard");
        let address = data['socket-address']
        this.viewState.shardDict[shard_id].push(address)
        if (!(data['broadcasted'])){
            data = {...data, broadcasted:true}
            await this.messenger.broadcastSequential(this.viewState.view.filter(a=>a!=address), "key-value-store-shard/add-member/"+shard_id, "PUT",data,  (res)=>{
                console.log("PUT member broadcast success");
            });
            let view_state_json = {"shard_count":this.viewState.shard_count, "shardDict":this.viewState.shardDict,  "shard_id":shard_id}
            await this.messenger.sendAndDetectCrash(address, "key-value-store-shard/view-state", "PUT", view_state_json, (res)=>{
                console.log("PUT member send view success");
            });
        }
        resJSON['statusCode'] = 200
        return resJSON
	}

    async handlePutViewState(data){
        let resJSON = {}
        console.log("PUT view state in shard");
        this.viewState.shard_count = data['shard_count']
        this.viewState.shardDict = data['shardDict']
        if ( data['shard_id'])
            this.viewState.myShardID = data['shard_id']
        else
            this.viewState.myShardID  = this.viewState.view.indexOf(this.viewState.socket_address) % this.viewState.shard_count
        this.viewState.myShard = this.viewState.shardDict[this.viewState.myShardID]
        for( const node of this.viewState.myShard){
			if (!this.storeHandler.cur_VC[node]) this.storeHandler.cur_VC[node] = 0
		}
       // console.log(" this.viewState.myShard ",  this.viewState.myShard, "this.viewState.shardDict", this.viewState.shardDict)
        if(data['shuffle_store'])
            await this.shuffle_store(data['shard_count'])
        resJSON['statusCode'] = 200
        return resJSON
    }

	async handlePutReshard(data){
        let shard_count = data['shard-count']
		let resJSON = {}
        console.log("PUT reshard");
        if (this.viewState.view.length/shard_count < 2){
            resJSON['statusCode'] = 400
            resJSON['body'] = {message:"Not enough nodes to provide fault-tolerance with the given shard count!"}
        }
        else if (shard_count== this.viewState.shard_count){
            resJSON['statusCode'] = 200
            resJSON['body'] = {message:"Resharding done successfully"}
        }
        else{
            this.viewState.shard_count = shard_count
            this.viewState.assign_shards()
            //console.log( "new shardDict", this.viewState.shardDict)
            let view_state_json = {"shard_count":this.viewState.shard_count, "shardDict":this.viewState.shardDict, "shuffle_store":true}
            await this.messenger.broadcastSequential(this.viewState.view, "key-value-store-shard/view-state", "PUT", view_state_json,(res) => {
                console.log("broadcast of changing shard count", shard_count, " successful");
            });
           
            await this.shuffle_store(shard_count)
            resJSON['statusCode'] = 200
            resJSON['body'] = {message:"Resharding done successfully"}
        }
        return resJSON
	}

    async shuffle_store(shard_count){
        let keys_to_transfer = []
        for (let i=0;i<shard_count;i++) keys_to_transfer[i] = {}
        for (const key in this.storeHandler.kvstore){
            let dest = this.viewState.keyToShardID(key)
            if(dest!=this.viewState.myShardID){
                //console.log("adding "+key+" to shard "+ dest);
                keys_to_transfer[dest][key] = this.storeHandler.kvstore[key]
                delete this.storeHandler.kvstore[key]
            }
        }
         let promises = keys_to_transfer.map(async (keys, shard_id)=>{
            if(keys){
                return await this.messenger.broadcastSequential(this.viewState.shardDict[shard_id], "key-value-store-shard/key-transfer", "PUT", {keys:keys, 'socket-address':this.viewState.socket_address}, (response) => {
                    console.log("transfer keys to shard", shard_id, "succeeded")
                })
            }
            else
                return Promise.resolve()
        })
        await Promise.all(promises)
    } 

    handleKeyTransfer(data){
        console.log("count", this.viewState.shard_count)
        let keys = data['keys']
        let resJSON = {}
        console.log("PUT key transfer from", data['socket-address'], ",keys:", Object.keys(keys));
        this.storeHandler.kvstore= {...this.storeHandler.kvstore, ...keys}  //put keys inside
        resJSON['statusCode'] = 200
        resJSON['body'] = {message:"keys transfered successfully"}
        return resJSON
    }

}