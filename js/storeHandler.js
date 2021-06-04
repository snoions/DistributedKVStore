const util = require('./util.js')
module.exports =  class StoreHandler{
	constructor(kvstore, viewHandler, shardHandler, messenger){
		this.kvstore = kvstore;
		this.viewHandler = viewHandler;
        this.shardHandler = shardHandler
        this.messenger = messenger
		this.delayed_reqs = [];  //store requests that are not yet deliverable (waiting for requests that happened before )
		this.cur_VC = {};  //point-wise maximum of of all delivered VCs, decides if a new request is deliverable
	}


	async handleReq(key, dataJSON, method, sendRes){
		if(!(this.shardHandler.inThisShard(key))){
            await this.forwardToShard(key, method,dataJSON, sendRes)
            return;
        }
        let metadata = dataJSON['causal-metadata']
        let resJSON = {}
        let broadcastedFrom = dataJSON['broadcastedFrom']
        if(!this.deliverable(metadata, broadcastedFrom))
            await this.gossiping() //get update-to-date kv pairs from other replicas
		if (this.deliverable(metadata, broadcastedFrom)){
            if(broadcastedFrom){
                console.log('broadcasted')
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

    async gossiping(){
        console.log("gossiping to get up-to-date kv pairs")
        let promises = await this.shardHandler.myShard.map(async (address)=>{
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
        await this.messenger.broadcastUntilSuccess(this.shardHandler.myShard, "key-value-store/"+key, method, data, (response) => {
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

    handleGetAll(sendRes){
        let resJSON = {}
        console.log("GET all kv pairs cur_VC:",this.cur_VC);
        resJSON['statusCode'] = 200
        resJSON['body'] = {kvstore:this.kvstore, cur_VC:this.cur_VC}
        sendRes(resJSON);
    }

 	handleGet(key, dataJSON){
		let resJSON = {}
		let metadata = dataJSON['causal-metadata']
		console.log("GET key="+key);
		if(key in this.kvstore && this.kvstore[key]['value']!=null){
			let {value, VC} = this.kvstore[key]
            VC =  util.pntwiseMax(VC, this.cur_VC)
            resJSON['statusCode'] = 200
            resJSON['body'] = {doesExist: true, message: "Retrieved successfully", value: value,
                                'causal-metadata': VC }
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

            this.kvstore[key] = {value:value, VC: VC}

            this.messenger.broadcast(this.shardHandler.myShard,"key-value-store/"+key, "PUT", { 'causal-metadata': this.cur_VC, broadcastedFrom:this.viewHandler.socket_address, value:value }, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })
            resJSON['body']['causal-metadata'] = VC
            resJSON['body']['shard-id'] = this.shardHandler.myShard
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

            this.messenger.broadcast(this.shardHandler.myShard, "key-value-store/"+key, "DELETE", { 'causal-metadata': this.cur_VC, broadcastedFrom:this.viewHandler.socket_address}, (response) => {
                console.log("broadcast to", response.config.url, "succeeded, response=", response.data)
            })

            resJSON['body']['causal-metadata'] = VC
        }
        return resJSON
	}

	async deliverable (VC, BroadCastedFrom){
	    console.log("VC: ", VC, "cur_VC: ", this.cur_VC)
        for (const key in VC){
            if(!(this.shardHandler.myShard.includes(key)))
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
		VC[this.viewHandler.socket_address]++
	}

};