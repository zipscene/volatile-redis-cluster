// Client for machines using the cluster.
// All this really has to do is keep an updated list of current cluster nodes, manage the connections, and hash keys on request

var EventEmitter = require('events').EventEmitter;
var ConsistentHashing = require('./consistent-hashing');
var util = require('util');
var redis = require('redis');

function ClusterClient(coordinatorConfig, options) {
	var self = this;

	this.coordinatorConfig = coordinatorConfig;
	this.options = options || {};

	this.channel = 'RedisVolatileClusterBus';
	this.keyPrefix = 'rvolcluster:';
	this.nodeSetKey = this.keyPrefix + 'nodeset';

	this.coordClientMBus = redis.createClient(coordinatorConfig.port, coordinatorConfig.host, coordinatorConfig);
	this.coordClientMain = redis.createClient(coordinatorConfig.port, coordinatorConfig.host, coordinatorConfig);
	this.coordClientMBus.on('error', function(error) { self.emit('error', error); });
	this.coordClientMain.on('error', function(error) { self.emit('error', error); });

	this.consistentHash = new ConsistentHashing(options);

	this.clientPool = {};

	this.isInitialized = false;
	this._init();
}
util.inherits(ClusterClient, EventEmitter);
module.exports = ClusterClient;

ClusterClient.prototype._init = function() {
	var self = this;
	var channel = self.channel;

	self.coordClientMBus.on('message', function(messageChannel, message) {
		if(messageChannel != channel) return;
		message = JSON.parse(message);
		if(message.type == 'update_nodes') {
			self._updateFromNodeMap(message.nodeMap);
		} else {
			self.emit('error', new Error('Unknown redis cluster message type: ' + message.type));
		}
	});

	self.coordClientMBus.once('subscribe', function() {
		self.coordClientMain.hgetall(self.nodeSetKey, function(error, nodeMap) {
			if(error) return self.emit('error', error);
			if(!nodeMap) nodeMap = {};
			self._updateFromNodeMap(nodeMap);
			self.isInitialized = true;
			self.emit('init');
		});
	});
	self.coordClientMBus.subscribe(channel);
};

ClusterClient.prototype._waitForInit = function(cb) {
	if(this.isInitialized) return cb();
	this.once('init', cb);
};

ClusterClient.prototype._updateFromNodeMap = function(nodeMap) {
	var self = this;
	self.consistentHash.clear();
	Object.keys(nodeMap).forEach(function(name) {
		var data = nodeMap[name];
		if(typeof data == 'string') data = JSON.parse(data);
		if(data.up) {
			self.consistentHash.addNode(name, data.weight, data.stateChangeTime, data);
		} else {
			self.consistentHash.addDownNode(name, data.weight, data.stateChangeTime, data);
		}
	});
	Object.keys(self.clientPool).forEach(function(name) {
		if(!self.consistentHash.nodes[name] || !self.consistentHash.nodes[name].up) {
			self.clientPool[name].end();
			delete self.clientPool[name];
		}
	});
};

ClusterClient.prototype.getShardData = function(key, options, cb) {
	if(typeof options == 'function') { cb = options; options = null; }
	var self = this;
	self._waitForInit(function() {
		var node = self.consistentHash.hashToNode(key, options);
		cb(null, node);
	});
};

ClusterClient.prototype.getShardClientConfig = function(key, options, cb) {
	if(typeof options == 'function') { cb = options; options = null; }
	var self = this;
	self.getShardData(key, options, function(error, node) {
		if(error) return cb(error);
		if(!node) return cb(null, null, null);
		if(!node.up) return cb(null, null, null);
		if(options && options.downNodeExpiry && node.timeSinceLastChange < options.downNodeExpiry * 1000) {
			return cb(null, null, null);
		}
		cb(null, node.data && node.data.config, node);
	});
};

ClusterClient.prototype.getShardClient = function(key, options, cb) {
	if(typeof options == 'function') { cb = options; options = null; }
	var self = this;
	self.getShardClientConfig(key, options, function(error, config, node) {
		if(error) return cb(error);
		if(!node || !config) return cb(null, null);
		if(self.clientPool[node.name]) return cb(null, self.clientPool[node.name]);
		var newClient = redis.createClient(config.port, config.host, config);
		self.clientPool[node.name] = newClient;
		newClient.on('error', function(error) {
			self.emit('slaveerror', error);
		});
		cb(null, newClient, node);
	});
};


