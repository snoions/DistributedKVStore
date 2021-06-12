var jumphash = require('jumphash')

module.exports =  class viewState{

    constructor(view, socket_address, shard_count){
        this.view = view
        this.shard_count = shard_count
        this.socket_address = socket_address
        this.shardDict = {}
        if (!shard_count)
            return
        view.forEach((address, index )=> {
            let shardID = index % this.shard_count
            if (!this.shardDict[shardID]) this.shardDict[shardID]=[]
            this.shardDict[shardID].push(address)
            if (address==this.socket_address) this.myShardID= shardID
        })
        console.log("shardDict:", this.shardDict)

        if (this.myShardID == undefined)
            throw new Error("socket address", socket_address, " not in view", view); 
        this.myShard = this.shardDict[this.myShardID]
    }


	keyToShardID(key){
        return jumphash(key,this.shard_count);
	}

    keyToShard(key){
        return this.shardDict[keyToShardID(key)];
	}

	inThisShard(key){
	    return this.myShardID == this.keyToShardID(key);
	}
}