module.exports = function(RED) {
    "use strict";
    var DeepstreamClient = require('deepstream.io-client-js');
    var querystring = require('querystring');

    function DeepstreamServerNode(config) {
        RED.nodes.createNode(this, config);
        this.server = config.server;
        this.port = config.port;
        this.name = config.name;
		this.methode = config.method;
        this.username = this.credentials.user;
        this.password = this.credentials.password;
		this.subscriptionTimeout = config.subscriptionTimeout || 500;
		this.rpcAckTimeout = config.rpcAckTimeout || 6000;
		this.rpcResponseTimeout = config.rpcResponseTimeout || 10000;
    }
	
    RED.nodes.registerType("deepstream-server", DeepstreamServerNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });

	function createDSClient(node, config, callback) {
		try {
			if (!node.client) {
				node.server = config.server;
				node.topic = config.topic;				
				node.serverConfig = RED.nodes.getNode(node.server);					
				
				var dsServer = node.serverConfig.server;
				if ( node.serverConfig.port ) {
					dsServer += ':' + node.serverConfig.port
				}
				
				var options = {};
				if ( node.serverConfig.rpcAckTimeout ) {
					options.rpcAckTimeout = node.serverConfig.rpcAckTimeout;
				}
				
				if ( node.serverConfig.rpcResponseTimeout ) {
					options.rpcAckTimeout = node.serverConfig.rpcResponseTimeout;
				}
				
				if ( node.serverConfig.subscriptionTimeout ) {
					options.rpcAckTimeout = node.serverConfig.subscriptionTimeout;
				}
				
				node.client = new DeepstreamClient(dsServer, options);	
				
				node.client.on("error", function(error) {
					node.status({fill:"grey",shape:"dot",text:"error - " + error});
					node.warn(error);
					try {
						node.client.close();
					} catch (err) { 
						console.log('Error - ' + JSON.stringify(err)); 
					}
					delete node.client;
					console.log('Reconnection in 5 secondes'); 
					setTimeout(createDSClient, 5000, node, config, callback);
				});			
				deepstreamLogin(node, callback);				
			} else {
				if (node.client.getConnectionState() !== 'OPEN') {
					deepstreamLogin(node, callback);
				} else {
					callback(node.client);
				}
			}
		} catch (err) {
			node.error(err);
		}
	}
	
	function deepstreamLogin(node, callback) {
		node.client.login(node.serverConfig, (success, data) => {
			if (success) {						
				if (typeof callback === 'function') {
					callback(node.client);
				}
			} else {
				node.status({fill:"red",shape:"ring",text:"error - " + data});
				node.warn("Error while login in", data);							
			}
		});
	}
	
	/****************************** RPC MAKE *******************************/
    function DeepstreamRpcMakeNode(config) {
        var node = this;
		RED.nodes.createNode(node, config);		
				
		node.on("input", function(msg) {
			createDSClient(node, config, function(client) {
				try {
					var method = msg.topic || config.method; 
					client.rpc.make(method, msg.payload, function(error, result){
						if (error) {
							node.error("Error while RPC Make - " + error);			
						} else {
							msg.payload = result;
							node.send(msg);
						}
					});
				} catch(err) {
					node.error(err);
				}
			});
        });			        

		node.on("error", function(error) {
			node.error("DeepstreamRpcMakeNode Error - " + error);
        });
		
        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });
    }
	
	/****************************** RPC PROVIDE *******************************/
		
    function DeepstreamRpcProvideNode(config) {
        var node = this;
		RED.nodes.createNode(node, config);
		
		node.status({fill:"grey",shape:"ring",text:"connecting"});
		createDSClient(node, config, function(client) {
			try {
				node.status({fill:"green",shape:"dot",text:"connected"});
				var method = config.method; 
				client.rpc.provide(method, function() {		
					var msg = {
						res : arguments[arguments.length-1],
						payload : []
					};
					
					for(var i in arguments) {
						if (i <  arguments.length-1) {
							msg.payload.push(arguments[i]);
						} else {
							break;
						}						
					}	
						
					node.send(msg);							
				});
			} catch(err) {
				node.error(err);
			}
		});		     
		
		node.on("error", function(error) {
			node.error("DeepstreamRpcProvideNode Error - " + error);
        });
		
        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });
    }
	
	/****************************** RPC RESPONSE *******************************/
		
    function DeepstreamRpcResponseNode(config) {
        var node = this;
		RED.nodes.createNode(node, config);

		node.on("input", function(msg) {
			try {				
				if (!msg.res) {
					node.error('Error, no res in message');
					node.status({fill:"red",shape:"dot",text:"no response in msg"});
				} else {
					if (msg.err) {
						msg.res.error(msg.err);
					} else {
						msg.res.send(msg.payload);
					}
				}		 	
			} catch (err) {
				node.error(err);
			}						
		});
		
		node.on("error", function(error) {
			node.error("DeepstreamRpcResponseNode Error - " + error);
        });
    }
	
	/****************************** Event emit *******************************/
    function DeepstreamEventEmitNode(config) {
        var node = this;
		RED.nodes.createNode(node, config);		
				
		node.on("input", function(msg) {
			createDSClient(node, config, function(client) {
				try {
					client.event.emit((msg.topic || config.method), msg.payload);
					node.send(msg);
				} catch(err) {
					node.error(err);
				}
			});
        });			        

        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });
    }
    
	
	/****************************** Event subscribe *******************************/
		
    function DeepstreamEventSubscribeNode(config) {
        var node = this;
		RED.nodes.createNode(node, config);
		
		node.status({fill:"grey",shape:"ring",text:"connecting"});
		createDSClient(node, config, function(client) {
			try {
				node.status({fill:"green",shape:"dot",text:"connected"});
				var method = config.method; 
				client.event.subscribe(method, function(data) {						
					var msg = {
						payload : data
					}
					node.send(msg);							
				});				
			} catch(err) {
				node.error(err);
			}
		});		     

        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });		
    }
	
	/****************************** Record update *******************************/
    function DeepstreamRecordUpdateNode(config) {
        var node = this;
		RED.nodes.createNode(node, config);		
				
		node.on("input", function(msg) {
			createDSClient(node, config, function(client) {
				try {					
					var record = client.record.getRecord(config.recordPath);					
					if (config.path) {
						record.set(config.path, msg.payload);
					} else {
						record.set(msg.payload);
					}	
					node.status({});
				} catch(err) {
					node.error(err);
				}
			});
        });			        

        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });
    }
    
	
	/****************************** Record subscribe *******************************/		
    function DeepstreamRecordSubscribeNode(config) {    
		var node = this;
		RED.nodes.createNode(node, config);		
		
		node.on("input", function(msg) {
			node.status({fill:"grey",shape:"ring",text:"connecting"});
			createDSClient(node, config, function(client) {
				try {
					node.status({fill:"green",shape:"dot",text:"connected"});
					var record = client.record.getRecord(config.recordPath);
					node.send({
						'topic'   : 'record',
						'payload' : record.get()
					})
					if (config.path) {				
						record.subscribe(config.path, function(data) {					
							var msg = {
								'topic'   : 'update',
								'payload' : data
							}
							node.send(msg);							
						});
					} else {
						record.subscribe( function(data) {					
							var msg = {
								'topic'   : 'update',
								'payload' : data
							}
							node.send(msg);							
						});
					}
				} catch(err) {
					node.error(err);
				}
			});		
		});			

        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });
    }
	
	/****************************** Record Get *******************************/		
    function DeepstreamRecordGetNode(config) {    
		var node = this;
		RED.nodes.createNode(node, config);		
				
		node.on("input", function(msg) {
			node.status({fill:"grey",shape:"ring",text:"connecting"});
			createDSClient(node, config, function(client) {
				try {
					node.status({fill:"green",shape:"dot",text:"connected"});
					var record = client.record.getRecord(config.recordPath);
					var msg = {
						'payload' : record.get()
					};				
					node.send(msg);	
					
				} catch(err) {
					node.error(err);
				}
			});		     
		});

        node.on("close", function(done) {
            if (node.client) {
                node.client.close();
				delete node.client;
            }
            done();
        });
    }
	
	
	/****************************** Register *******************************/		
    RED.nodes.registerType("Deepstream RPC make",DeepstreamRpcMakeNode);
    RED.nodes.registerType("Deepstream RPC provide",DeepstreamRpcProvideNode);
    RED.nodes.registerType("Deepstream RPC response",DeepstreamRpcResponseNode);
	
    RED.nodes.registerType("Deepstream Event emit",DeepstreamEventEmitNode);
	RED.nodes.registerType("Deepstream Event subscribe",DeepstreamEventSubscribeNode);
	
	RED.nodes.registerType("Deepstream Record get",DeepstreamRecordGetNode);
	RED.nodes.registerType("Deepstream Record update",DeepstreamRecordUpdateNode);
	RED.nodes.registerType("Deepstream Record subscribe",DeepstreamRecordSubscribeNode);

};