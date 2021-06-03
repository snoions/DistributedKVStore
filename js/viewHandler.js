const util = require("./util.js")
const axios = require('axios');
module.exports =  class ViewHandler{
    constructor(view, socket_address, crash_threshold){
		this.view = view;
		this.socket_address = socket_address;
        this.crash_threshold = crash_threshold;
	}

    handleReq(address, method, sendRes){
		let resJSON = {}
		if(method =="GET") {
			resJSON = this.handleGet();
		}else if(method =="PUT"){
			resJSON = this.handlePut(address);
		}
		else if (method =='DELETE'){
			resJSON = this.handleDelete(address);
		}
		sendRes(resJSON);
	}

    handleGet(){
		let resJSON = {}
		console.log("View GET");
		resJSON['statusCode'] = 200
		resJSON['body'] = {message:"View retrieved successfully", view: this.view.join()}
		return resJSON
	}

	handlePut(address){
		let resJSON = {}
		console.log("View PUT "+ address)
		if (this.view.includes(address)){
			resJSON['statusCode'] = 404
			resJSON['body'] = {error: "Socket address already exists in the view", message: "Error in PUT"}
		}else{
			this.view.push(address)
			resJSON['statusCode'] = 201
			resJSON['body'] = { message: "Replica added successfully to the view"}
		}
		return resJSON
	}

	handleDelete(address){
		let resJSON = {}
		console.log("View DELETE "+ address)
		if (!this.view.includes(address)){
			resJSON['statusCode'] = 404
			resJSON['body'] = {error: "Socket address does not exist in the view", message: "Error in DELETE"}
		}else{
			this.view = this.view.filter(a => a!=address )
			resJSON['statusCode'] = 201
			resJSON['body'] = { message: "Replica deleted successfully from the view"}
		}
		return resJSON
	}

    broadcast(endpoint, method, data, thenFunc){
    		const view_other = this.view.filter(address => address!=this.socket_address );  //view excluding the address of the current replica
    		console.log("in broadcast, view_other=", view_other)
    		for (let address of view_other) {
    		    this.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
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
                    if (count==this.crash_threshhold){
                        console.log(address+" crashed")
                        //delete node in view and shardDict
                        this.view = this.view.filter(a => a!=address )
                        for (let shard of Object.values(this.shardDict)) {
                            shard =shard.filter(a => a!=address )
                        }

                        this.broadcast('key-value-store-view', 'DELETE', {"socket-address": address})  //anounce to all replicas that a replica crashed
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