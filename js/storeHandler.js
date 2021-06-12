const util = require('./util.js')
module.exports =  class StoreHandler{
	constructor(kvstore, viewState, messenger){
		this.kvstore = kvstore;
		this.viewState = viewState;
        this.messenger = messenger
		this.delayed_reqs = [];  //store requests that are not yet deliverable (waiting for requests that happened before )
        this.cur_VC = {}
        for( const node of viewState.myShard){
			this.cur_VC[node] = 0
		}
		this.shardHandler;
	}


	async handleReq(key, dataJSON, method, sendRes){
		if(!(this.viewState.inThisShard(key))){
            await this.forwardToShard(key, method,dataJSON, sendRes)
            return;
        }
        let metadata = dataJSON['causal-metadata']
        let resJSON = {}
        let broadcastedFrom = dataJSON['broadcasted-from']
        if(!this.deliverable(metadata, broadcastedFrom))
            await this.gossip() //get update-to-date kv pairs from other replicas
		if (this.deliverable(metadata, broadcastedFrom)){
            if(broadcastedFrom){
                resJSON = this.handleBroadcastedReq(key, dataJSON, method)
            }
            else if(method =="GET") {
                resJSON = this.handleGet(key, dataJSON);
            }
            else if(method =="PUT"){
                resJSON = this.handlePut(key,dataJSON);
            }
            else if (method =='DELETE'){
                resJSON = this.handleDelete(key,dataJSON);
            }
            sendRes(resJSON);
        }
        else{
            resJSON['statusCode'] = 503
            resJSON['body'] = {message: "metadata error", error: "message currently not deliverable", 'causal-metadata': metadata,
                            'currVC':this.cur_VC }
            sendRes(resJSON);
        }
        console.log("updated cur_VC", this.cur_VC)

	}

    async gossip(){
        console.log("gossiping to get up-to-date kv pairs")
        let promises = await this.viewState.myShard.map(async (address)=>{
            return await this.messenger.sendAndDetectCrash(address, "key-value-store-all", "GET", {}, (response) => {
                let {kvstore, cur_VC} = response.data
                console.log("gossiping succeeded, replica VC=", cur_VC)
                if(!(this.deliverable(cur_VC))){
                    for (let [key, entry] of Object.entries(kvstore)){
                        let VC = entry['VC']
                        if (!(this.deliverable(VC))){
                            this.kvstore[key] = entry
                            console.log("in gossip, key",key, "updated to",entry)
                        }
                    }
                }
                this.cur_VC = util.pntwiseMax( this.cur_VC , cur_VC);
            })
        })
        await Promise.all(promises)
    }

	async forwardToShard(key, method, data, sendRes){
        let resJSON = {}
        let shardID = this.viewState.keyToShardID(key)
        await this.messenger.broadcastUntilSuccess(this.viewState.shardDict[shardID], "key-value-store/"+key, method, data, (response) => {
            console.log("forward to", response.config.url, " of shard",shardID, "succeeded, response=", response.data)
            resJSON['body'] = response.data
            resJSON['statusCode'] = response.status
        })
        if(resJSON){
            sendRes(resJSON)
        }
        else{
            resJSON['statusCode'] = 503
            resJSON['body'] = {message: "shard not available", error: "all nodes in the target shard is down", 'shardID':shardID}
            sendRes(resJSON);
        }
    }

    handleBroadcastedReq(key, dataJSON, method){
        let VC = dataJSON['causal-metadata']
        let resJSON = {}
        if(method=="PUT"){
            let value = dataJSON['value']
            if(key in this.kvstore && this.kvstore[key]['value']){
                resJSON['statusCode'] = 200
                resJSON['body'] = {message: "Updated successfully", replaced: true}
            }
            else {
                resJSON['statusCode'] = 201
                resJSON['body'] = {message: "Added successfully", replaced: false}
            }
            this.kvstore[key] = {value:value, VC: VC}
        }
        if (method == "DELETE"){
            resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Deleted successfully"}
            this.kvstore[key] = {value:null, VC: VC}
            console.log("deleted in broadcasted")
        }
        this.cur_VC = util.pntwiseMax(this.cur_VC , VC)
        return resJSON
    }

    async handleGetAll(dataJSON, sendRes){
        if(dataJSON['gossip'])
            await this.gossip()
        let resJSON = {}
        console.log("GET all kv pairs cur_VC:",this.cur_VC);
        resJSON['statusCode'] = 200
        resJSON['body'] = {kvstore:this.kvstore, cur_VC:this.cur_VC}
        sendRes(resJSON);
    }

 	handleGet(key, dataJSON){
		let resJSON = {}
		let reqVC = dataJSON['causal-metadata']
		console.log("GET key="+key);
		if(key in this.kvstore && this.kvstore[key]['value']!=null){
			let {value, VC} = this.kvstore[key]
            reqVC =  util.pntwiseMax(VC, reqVC)
            resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Retrieved successfully", value: value,
                                'causal-metadata': reqVC }
		}
		else {
			resJSON['statusCode'] = 404
            resJSON['body'] = {doesExist: false, message: "Error in GET", error: "Key does not exist",
                                'causal-metadata': reqVC }
		}
		return resJSON
	}

	handlePut(key, dataJSON){
	    let value = dataJSON['value'];
        let VC = dataJSON['causal-metadata']
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
			if(key in this.kvstore && this.kvstore[key]['value']!=null){
				resJSON['statusCode'] = 200
				resJSON['body'] = {message: "Updated successfully", replaced: true}
			}
			else {
				resJSON['statusCode'] = 201
				resJSON['body'] = {message: "Added successfully", replaced: false}
			}
			this.increment_cur_address(this.cur_VC)
            VC = util.pntwiseMax(this.cur_VC, VC)
            console.log("VC", VC)
            this.kvstore[key] = {value:value, VC: VC}

            this.messenger.broadcast(this.viewState.myShard,"key-value-store/"+key, "PUT", { 'causal-metadata': VC, 'broadcasted-from':this.viewState.socket_address, value:value }, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })
            resJSON['body']['causal-metadata'] = VC
            resJSON['body']['shard-id'] = this.viewState.myShardID
		}
		return resJSON
	}

	handleDelete(key, dataJSON){
		let resJSON = {}
        let VC = dataJSON['causal-metadata']
		console.log("DELETE key="+key);
		if(key in this.kvstore == false|| this.kvstore[key]['value']==null){
            resJSON['statusCode'] = 404
            resJSON['body'] = {doesExist: false, message: "Error in DELETE", error: "Key does not exist"}
        }
        else {
            resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Deleted successfully"}

            this.increment_cur_address(this.cur_VC)
            VC = util.pntwiseMax(this.cur_VC, VC)


            this.kvstore[key] = {value:null, VC: VC}

            this.messenger.broadcast(this.viewState.myShard, "key-value-store/"+key, "DELETE", { 'causal-metadata': VC, 'broadcasted-from':this.viewState.socket_address}, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })

            resJSON['body']['causal-metadata'] = VC
            resJSON['body']['shard-id'] = this.viewState.myShardID
        }
        return resJSON
	}

	deliverable (VC, BroadCastedFrom){
	    //console.log("VC: ", VC, "cur_VC: ", this.cur_VC)
        for (const key in VC){
            if(!(this.viewState.myShard.includes(key)))
                continue
            if(key==BroadCastedFrom && VC[key]!=this.cur_VC[key]+1)
                return false
            else if (!(key in this.cur_VC) )
                return false
            else if (VC[key]>this.cur_VC[key])
                return false
        }
        return true
    }

    increment_cur_address(VC){
		VC[this.viewState.socket_address]++
	}

};