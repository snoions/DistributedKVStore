const http = require('http');
const axios = require('axios');
const util = require('./util.js')
const hostname = '0.0.0.0';
const port = 8085;
const crash_threshold = 3;

var kvstore = {}

if (!process.env.VIEW || !process.env.SOCKET_ADDRESS ){
  throw new Error("missing environment variables VIEW or SOCKET_ADDRESS ");
}
var view = process.env.VIEW.split(",")
const socket_address = process.env.SOCKET_ADDRESS
var shard_count = process.env.SHARD_COUNT


const StoreHandler = require('./storeHandler.js');
const ViewHandler = require('./viewHandler.js');
const ShardHandler = require('./shardHandler.js');
const Messenger = require('./messenger.js')
const ViewState = require('./viewState.js')
const viewState = new ViewState(view, socket_address, shard_count)
const messenger = new Messenger(viewState, crash_threshold);
const viewHandler = new ViewHandler(viewState, messenger);
const storeHandler = new StoreHandler(kvstore, viewState, messenger);
const shardHandler = new ShardHandler( viewState, storeHandler, messenger);

const server = http.createServer((req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.statusCode = 405;
  let resJSON = {}
  let urlComponents =req.url.split("/");
  urlComponents[urlComponents.length-1] =urlComponents[urlComponents.length-1].split("?")[0]   //strip querystring

  let data = ''
  let dataJSON = {}
  req.on('error', (err) => {
      console.error(err);
  }).on('data', (chunk) => {
      data+=chunk;
  }).on('end', () => {
  	if (data)
        dataJSON = JSON.parse(data);

    console.log("incoming", req.method, "to",req.url, ",data:", dataJSON)
    let sendRes = function(resJSON) {
      if (resJSON.statusCode)
        res.statusCode = resJSON.statusCode;
      res.end(JSON.stringify(resJSON.body));
    };
    //console.log("urlComponents: "+ urlComponents);
    if(urlComponents.length>2 && urlComponents[1]=="key-value-store"){
      let key = urlComponents[2];
      //console.log("gonna go to hadler")
      storeHandler.handleReq(key, dataJSON, req.method, sendRes);
    }
    else if(urlComponents.length>1 && urlComponents[1]=="key-value-store-view"){
      let address = dataJSON['socket-address'];
      viewHandler.handleReq(address, req.method, sendRes);
    }
    else if(urlComponents.length>1 && urlComponents[1]=="key-value-store-all" && req.method=="GET"){
       storeHandler.handleGetAll(dataJSON, sendRes);
    }
    else if (urlComponents.length>2 && urlComponents[1]=="key-value-store-shard"){
        let shard_id = -1;
        let func = urlComponents[2];
        console.log("components length: "+ urlComponents.length+ "url: "+ urlComponents)
        if (urlComponents.length >= 3){
            shard_id = parseInt(urlComponents[3]);
        }
        shardHandler.handleReq(func, shard_id, req.method, dataJSON, sendRes);
    }

  });

 });


server.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
  //wait for other replicas to start up
  setTimeout(initializeReplica, 2000)
});



async function initializeReplica() {
  // broadcast its address to other replicas
  messenger.broadcast(viewState.view, "key-value-store-view", "PUT", {"socket-address":socket_address}, (response) => {
    console.log("broadcast to", response.config.url, "succeded, response=",response.data)
  })

  messenger.broadcastUntilSuccess( viewState.myShard, "key-value-store-all", "GET", {}, (response) => {
    let {kvstore, cur_VC} = response.data
    console.log("get all kv-pairs succeeded, kvstore",kvstore,"cur_VC=",cur_VC )
    //in case some put requests have already been delivered in this replica
    storeHandler.kvstore = {...kvstore}
    storeHandler.cur_VC = util.pntwiseMax( storeHandler.cur_VC , cur_VC);
  })
}