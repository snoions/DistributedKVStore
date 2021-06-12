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
		        resJSON = await this.handlePutReshard(data);
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
        if (!(data['broadcasted'])){
            data = {...data, broadcasted:true}
            await this.messenger.broadcastSequential(this.viewState.view, "key-value-store-shard/add-member/"+shard_id, "PUT",data,  (res)=>{
                console.log("PUT member broadcast success");
            });
            let view_state_json = {"shard_count":this.viewState.shard_count, "view": this.viewState.view,"shardDict":this.viewState.shardDict,  "shard_id":shard_id}
            await this.messenger.sendAndDetectCrash(address, "key-value-store-shard/view-state", "PUT", view_state_json, (res)=>{
                console.log("PUT member send view success");
            });
        }
        this.viewState.shardDict[shard_id].push(address)
        this.viewState.view.push(address)
        resJSON['statusCode'] = 200
        return resJSON
	}

    handlePutViewState(data){
        let resJSON = {}
        console.log("PUT view state in shard");
        this.viewState.shard_count = data['shard_count']
        this.viewState.view = data['view']
        this.viewState.shardDict = data['shardDict']
        this.viewState.myShardID = data['shard_id']
        this.viewState.myShard = this.viewState.shardDict[this.viewState.myShardID]
        resJSON['statusCode'] = 200
        return resJSON
    }

	async handlePutReshard(shard_count){
		let resJSON = {}
        console.log("PUT reshard");
        if (this.viewState.view.length/shard_count < 2){
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
           

            resJSON['statusCode'] = 200
            resJSON['body'] = {message:"Resharding done successfully"}
        }
        return resJSON
	}

}