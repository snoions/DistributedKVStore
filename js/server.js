const http = require('http');
const axios = require('axios');
const util = require('./util.js')
const hostname = '0.0.0.0';
const port = 8085;
const crash_threshhold = 1;

var kvstore = {}

if (!process.env.VIEW || !process.env.SOCKET_ADDRESS){
  throw new Error("missing environment variables VIEW or SOCKET_ADDRESS");
}
var view = process.env.VIEW.split(",")
const socket_address = process.env.SOCKET_ADDRESS
var index = 0
for (index=0; index<view.length; index++){
    if (view[index] == socket_address){
        break;
    }
}

const StoreHandler = require('./storeHandler.js');
const ViewHandler = require('./viewHandler.js');
const viewHandler = new ViewHandler(view, socket_address, crash_threshhold)
const storeHandler = new StoreHandler(kvstore, viewHandler, index);

const server = http.createServer((req, res) => {
  console.log("incoming", req.method, "to",req.url)
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

    let sendRes = function(resJSON) {
      if (resJSON.statusCode)
        res.statusCode = resJSON.statusCode;
      res.end(JSON.stringify(resJSON.body));
    };

    if(urlComponents.length>2 && urlComponents[1]=="key-value-store"){
      let key = urlComponents[2];
      storeHandler.handleReq(key, dataJSON, req.method, sendRes);
    }else if(urlComponents.length>1 && urlComponents[1]=="key-value-store-view"){
      let address = dataJSON['socket-address'];
      viewHandler.handleReq(address, req.method, sendRes);
    }
    else if(urlComponents.length>1 && urlComponents[1]=="key-value-store-all" && req.method=="GET"){
      storeHandler.handleGetAll(sendRes);
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
  viewHandler.broadcast("key-value-store-view", "PUT", {"socket-address":socket_address}, (response) => {
    console.log("broadcast to", response.config.url, "succeded, response=",response.data)
  })

  //try to get kv-pairs from another replica. Stop the iteration when succeeds
  let cont = true    
  const view_other = view.filter(address => address!=socket_address );
  for (address of view_other) {
    if (!cont)
      break;
    console.log("trying to get all kv-pairs from "+ address)
    await viewHandler.sendAndDetectCrash(address, "key-value-store-all", "GET", {}, (response) => {
        if(response.status==200){
            let {kvstore, cur_VC} = response.data
            console.log("get all kv-pairs succeeded, kvstore",kvstore,"cur_VC=",cur_VC )
            //in case some put requests have already been delivered in this replica
            storeHandler.kvstore = {...kvstore}
            storeHandler.cur_VC = storeHandler.pointwiseMax( storeHandler.cur_VC , cur_VC);
            cont = false;
        }else{
          console.log("get all kv-pairs received error, response=", response.data )
        }
      })
  }
}



