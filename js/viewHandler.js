module.exports =  class ViewHandler{
    constructor(viewState){
		this.viewState = viewState;
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
		resJSON['body'] = {message:"View retrieved successfully", view: this.viewState.view.join()}
		return resJSON
	}

	handlePut(address){
		let resJSON = {}
		console.log("View PUT "+ address)
		if (this.viewState.view.includes(address)){
			resJSON['statusCode'] = 404
			resJSON['body'] = {error: "Socket address already exists in the view", message: "Error in PUT"}
		}else{
			this.viewState.view.push(address)
			resJSON['statusCode'] = 201
			resJSON['body'] = { message: "Replica added successfully to the view"}
		}
		return resJSON
	}

	handleDelete(address){
		let resJSON = {}
		console.log("View DELETE "+ address)
		if (!this.viewState.view.includes(address)){
			resJSON['statusCode'] = 404
			resJSON['body'] = {error: "Socket address does not exist in the view", message: "Error in DELETE"}
		}else{
			this.viewState.view = this.viewState.view.filter(a => a!=address )
			resJSON['statusCode'] = 201
			resJSON['body'] = { message: "Replica deleted successfully from the view"}
		}
		return resJSON
	}


	
}