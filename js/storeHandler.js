const util = require('./util.js')
module.exports =  class StoreHandler{
	constructor(kvstore, viewHandler){
		this.kvstore = kvstore;
		this.viewHandler = viewHandler;
		this.delayed_reqs = [];  //store requests that are not yet deliverable (waiting for requests that happened before )
		this.cur_VC = {};  //point-wise maximum of of all delivered VCs, decides if a new request is deliverable
		this.client_count = 0;  //number of client
		this.shardHandler;
	}

    setShardHandlerInstance(shardHandler){
        this.shardHandler = shardHandler;
    }

	async handleReq(key, dataJSON, method, sendRes){
		if(!(this.shardHandler.inThisShard(key))){
            console.log("before forwardToShard")
            await this.forwardToShard(key, method,dataJSON, sendRes)
            return;
        }
        let metadata = dataJSON['causal-metadata']
        let resJSON = {}
        if(!this.deliverable(metadata) && util.partiallyGreater(metadata['VC'][this.shardHandler.shardID], this.cur_VC))
            await this.gossiping() //get update-to-date kv pairs from other replicas
		if (deliverable(metadata)){
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
		}
		else{
			resJSON['statusCode'] = 503
            resJSON['body'] = {message: "metadata error", error: "message currently not deliverable", 'causal-metadata': metadata,
                            'currVC':this.cur_VC }
            sendRes(resJSON);
		}
		console.log("updated cur_VC", this.cur_VC)
	}

    async gossiping(){
        console.log("gossiping to get up-to-date kv pairs")
        let shardMembers = this.shardHandler.handlerGetIdMembers(this.shardHandler.myShard)['body']['shard-id-members']
        let promises = await shardMembers.map(async (address)=>{
            return await this.viewHandler.sendAndDetectCrash(address, "key-value-store-all", "GET", {}, (response) => {
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
            })
        })
        await Promise.all(promises)
    }

	async forwardToShard(key, method, data, sendRes){
        let shardID = this.shardHandler.keyToShardID(key);
        let shard = this.shardHandler.handleGetIdMembers(shardID)['body']['shard-id-members'];
        let resJSON = {}
        await this.shardHandler.broadcastUntilSuccess(shard, "key-value-store/"+key, method, data, (response) => {
            console.log("forward to", response.config.url, " of shard", shardID , "succeeded, response=", response.data)
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
        let {VC, client_name} = dataJSON['causal-metadata']
        let shardVC = VC[this.shardHandler.myShard]
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
            this.kvstore[key]['value'] = {value:null, VC: VC}
        }

        this.cur_VC[client_name] = shardVC[client_name]

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
		if(key in this.kvstore && this.kvstore[key]['value']!=null){
			let {value, storeVC} = this.kvstore[key]
            if (!metadata){
                metadata = this.new_client_metadata()
            }
            let reqVC = metadata['VC']
            reqVC[this.shardHandler.myShard] = util.pntwiseMax(storeVC, reqVC[this.shardHandler.myShard] )
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
			if(key in this.kvstore && this.kvstore[key]['value']!=null){
				resJSON['statusCode'] = 200
				resJSON['body'] = {message: "Updated successfully", replaced: true}
			}
			else {
				resJSON['statusCode'] = 201
				resJSON['body'] = {message: "Added successfully", replaced: false}
			}
			if (!metadata)
				metadata = new_client_metadata()

			let {VC, client_name} = metadata
            let shardVC = VC[this.shardHandler.myShard]
            this.cur_VC[client_name] = shardVC[client_name]
            this.kvstore[key] = {value:value, VC: shardVC}

            this.shardHandler.broadcastInThisShard("key-value-store/"+key, "PUT", { 'causal-metadata': metadata, broadcasted:true, value:value }, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })
            shardVC[client_name]++
            resJSON['body']['causal-metadata'] = metadata
		}
		return resJSON
	}

	handleDelete(key, dataJSON){
	    let metadata = dataJSON['causal-metadata']
		let resJSON = {}
		console.log("DELETE key="+key);
		if(key in this.kvstore == false|| this.kvstore[key]['value']==null){
            resJSON['statusCode'] = 404
            resJSON['body'] = {doesExist: false, message: "Error in DELETE", error: "Key does not exist"}
        }
        else {
            resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Deleted successfully"}

            if (!metadata){
                metadata = this.new_client_metadata()
            }
            let {VC, client_name} = metadata
            let shardVC = VC[this.shardHandler.myShard]
            this.cur_VC[client_name] = shardVC[client_name]
            this.kvstore[key] = {value:null, VC: shardVC}

            this.shardHandler.broadcastInThisShard("key-value-store/"+key, "DELETE", { 'causal-metadata': metadata, broadcasted:true}, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })

            shardVC[client_name]++
            resJSON['body']['causal-metadata'] = metadata
        }
        return resJSON
	}

	deliverable (metadata){
        if (!metadata)
            return true
        let {VC, client_name} = metadata
        console.log("VC: ", VC, "cur_VC: ", this.cur_VC, 'client_name: ', client_name)
        let shardVC = VC[this.shardHandler.myShard]
        for (const key in shardVC){
            if (key == client_name && shardVC[key] != this.cur_VC[key]+1)
                return false
            if (key != client_name && shardVC[key]>this.cur_VC[key])
                return false
        }
        return true
    }

	new_client_metadata(){
        let client_name = this.viewHandler.socket_address+ "_"+ String(this.client_count)
        let VC = {}
        for(const shardID in this.shardHandler.handleGetIds()){
            VC[shardID]={}
            VC[shardID][client_name] = 1;
        }
        this.client_count++;
        return {client_name: client_name, VC: VC}
    }


};