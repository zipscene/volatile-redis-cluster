// The coordinator's job is to connect to the coordinator redis server and listen for status updates from slave nodes
// It also pings active slave nodes to determine if they've gone down
// This is a stand-alone executable.
// Command-line options are: -h HOST -p PORT (host and port of the coordinator redis server)

var redis = require('redis');
var async = require('async');

var argv = require('yargs')
	.default('h', '127.0.0.1')	// master redis instance host
	.default('p', 6379)			// master redis instance port
	.argv;

var coordinatorConfig = {
	host: argv.h,
	port: parseInt(argv.p, 10)
};

var mainClient = redis.createClient(coordinatorConfig.port, coordinatorConfig.host, coordinatorConfig);
var messageClient = redis.createClient(coordinatorConfig.port, coordinatorConfig.host, coordinatorConfig);
mainClient.on('error', function(error) { console.log(error); });
messageClient.on('error', function(error) { console.log(error); });

var nodeSetKey = 'rvolcluster:nodeset';
var mainBusChannel = 'RedisVolatileClusterBus';
var slaveBusChannel = 'RedisVolatileClusterSlaveBus';

var pingInterval = 5000;
var pingTimeout = 3000;

function getCurrentNodeMap(cb) {
	mainClient.hgetall(nodeSetKey, function(error, nodeMap) {
		if(error) return cb(error);
		if(!nodeMap) nodeMap = {};
		Object.keys(nodeMap).forEach(function(name) {
			nodeMap[name] = JSON.parse(nodeMap[name]);
			nodeMap[name].name = name;
			nodeMap[name].lastUp = new Date().getTime();
		});
		cb(null, nodeMap);
	});
}

var currentNodeMap = {};
var clientPool = {};

process.on('uncaughtException', function(error) {
	console.log('UNCAUGHT EXCEPTION');
	console.log(error);
	console.log(error.stack);
});

function sendUpdateNodes() {
	console.log('Sending update nodes.');
	//console.log('Node map is:');
	//console.log(JSON.stringify(currentNodeMap, null, 4));
	mainClient.publish(mainBusChannel, JSON.stringify( { type: 'update_nodes', nodeMap: currentNodeMap } ) );
}

function updatedNodeMapEntry(name) {
	if(currentNodeMap[name]) {
		mainClient.hset(nodeSetKey, name, JSON.stringify(currentNodeMap[name]), function(error) {
			if(error) console.log(error);
			sendUpdateNodes();
		});
	} else {
		mainClient.hdel(nodeSetKey, name, function(error) {
			if(error) console.log(error);
			sendUpdateNodes();
		});
	}
}

function getNodeClient(node) {
	var name = '' + node.config.host + ':' + node.config.port;
	if(clientPool[name]) return clientPool[name];
	clientPool[name] = redis.createClient(node.config.port, node.config.host, node.config);
	clientPool[name].on('error', function(error) {
		console.log('Error on client ' + name + ':');
		console.log(error);
		var nodeName = node.name || name;
		if(currentNodeMap[nodeName]) {
			nodeDown(currentNodeMap[nodeName]);
		}
	});
	return clientPool[name];
}

function deleteNodeClient(node) {
	var name = '' + node.config.host + ':' + node.config.port;
	if(clientPool[name]) {
		clientPool[name].end();
		delete clientPool[name];
	}
}

function nodeUp(node) {
	var name = node.name || ('' + node.config.host + ':' + node.config.port);
	if(currentNodeMap[name] && currentNodeMap[name].up) return currentNodeMap[name];
	if(currentNodeMap[name]) {
		currentNodeMap[name] = node;
		node.up = true;
		node.weight = node.weight || 1.0;
		node.stateChangeTime = new Date().getTime();
		node.name = name;
	} else {
		currentNodeMap[name] = node;
		node.up = true;
		node.weight = node.weight || 1.0;
		node.stateChangeTime = 1;
		node.name = name;
	}
	console.log('Node is up: ' + name);
	console.log('Flushing DB for slave ' + name);
	getNodeClient(currentNodeMap[name]).flushdb(function(error) { if(error) console.log('Error flushing db', error); });
	updatedNodeMapEntry(name);
	return currentNodeMap[name];
}

function nodeDown(node) {
	if(!node.up) return;
	var name = node.name;
	deleteNodeClient(node);
	currentNodeMap[name].up = false;
	currentNodeMap[name].stateChangeTime = new Date().getTime();
	updatedNodeMapEntry(name);
	setTimeout(function() {
		if(!currentNodeMap[name].up && new Date().getTime() - currentNodeMap[name].stateChangeTime >= 1200000) {
			delete currentNodeMap[name];
			updatedNodeMapEntry(name);
		}
	}, 1200000);
	console.log('Node is down: ' + name);
}

getCurrentNodeMap(function(error, nodeMap) {
	if(error) {
		console.log(error);
		process.exit(1);
	}
	currentNodeMap = nodeMap;

	messageClient.subscribe(slaveBusChannel);

	console.log('Initialized.');

	sendUpdateNodes();

	setInterval(function() {
		async.each(Object.keys(currentNodeMap), function(nodeName, cb) {
			var node = currentNodeMap[nodeName];
			if(!node.up) return cb();

			if(node.lastPing && node.lastPing + 45000 < new Date().getTime()) {
				console.log('Node down due to missed slave ping.');
				nodeDown(node);
				return;
			}

			var client = getNodeClient(node);
			var gotPong = false;
			var tmout = setTimeout(function() {
				if(!gotPong) {
					nodeDown(node);
				}
			}, pingTimeout);
			client.ping(function(error) {
				if(error) console.log(error);
				else {
					clearTimeout(tmout);
					nodeUp(node);
				}
			});
			cb();
		}, function(error) {
			if(error) {
				console.log(error);
			}
		});
	}, pingInterval);

	messageClient.on('message', function(channel, message) {
		message = JSON.parse(message);
		if(message.type == 'node_up') {
			var node = nodeUp(message.node);
			//console.log('Got ping from ' + node.name);
			node.lastPing = new Date().getTime();
		} else {
			console.log('Unknown message type', message);
		}
	});

});




