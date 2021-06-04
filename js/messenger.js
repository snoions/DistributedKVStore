const axios = require('axios');
module.exports =  class Messenger{
    setView(view){
        this.view = view
    }
    setShard(shard){
        this.shard = shard
    }

    async broadcast (view, endpoint, method, data, thenFunc){
        const view_other = view.filter(address => address!=this.socket_address );  //view excluding the address of the current replica
        console.log("in broadcast, view_other=", view_other)
        for (let address of view_other) {
            this.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
        }
    }

    async broadcastSequential (view, endpoint, method, data, thenFunc){
        const view_other = view.filter(address => address!=this.socket_address );  //view excluding the address of the current replica
        console.log("in broadcast, view_other=", view_other)
        for (let address of view_other) {
            await this.sendAndDetectCrash(address, endpoint, method, data, thenFunc)
        }
    }


    //broadcast to each address in a shard until success
    async broadcastUntilSuccess(view, endpoint, method, data, thenFunc){ 
        const view_others = view.filter(address => address!=this.socket_address );  //other replicas in the shard
        console.log("in BroadcastUntilSuccess, shard_others=", view_others)
        let cont = true
        for (let address of view_others) {
            if (!cont)
            break;
            await this.sendAndDetectCrash(address, endpoint, method, data, (response)=>{thenFunc(response);console.log("stop broadcast");cont=false})
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
                    if (thenFunc instanceof Function) thenFunc(error.response)
                } else if (error.request) {
                    // The request was made but no response was received
                    console.log("no response from ", address);
                    if (count==this.crash_threshold){
                        console.log(address+" crashed")
                        //delete node in view and shardDict
                        this.view = this.view.filter(a => a!=address )
                        this.shard = this.shard.filter(a => a!=address )
                        this.broadcast(this.view, 'key-value-store-view', 'DELETE', {"socket-address": address})  //anounce to all replicas that a replica crashed
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