// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

var seedRandom = require('seed-random');
var crc32 = require('buffer-crc32');

function ConsistentHash(options) {
	// Map from node names to node states: { up: true, stateChangeTime: Date, name: name }
	this.nodes = {};
	// Sorted list of buckets: { value: ..., name: nodename }
	this.buckets = [];
	if(!options) options = {};
	this.bucketsPerNode = options.bucketsPerNode || 16;
	this.downNodeExpiry = (options.downNodeExpiry || 0) * 1000;
	this.pruneInterval = (options.pruneInterval || 30) * 1000;
	this.lastPruneTime = new Date().getTime();
	this.numUpNodes = 0;
	this.nodeNumMap = []; // map from node numbers to node names
}

ConsistentHash.prototype.clear = function() {
	this.nodes = {};
	this.buckets = [];
	this.nodeNumMap = [];
	this.lastPruneTime = new Date().getTime();
	this.numUpNodes = 0;
};

ConsistentHash.prototype._updateNodeNumMap = function() {
	var self = this;
	this.nodeNumMap = Object.keys(self.nodes).sort().filter(function (key) {
		return !!self.nodes[key].up;
	});
};

ConsistentHash.prototype.addNode = function(name, weight, stateChangeTime, data) {
	var curTime = new Date().getTime();
	if(!stateChangeTime) stateChangeTime = curTime;
	if(!weight) weight = 1.0;
	if(this.nodes[name]) {
		if(this.nodes[name].weight != weight) throw new Error('Tried to add node ' + name + ' with different weight!');
		this.nodes[name].data = data;
		this.nodes[name].stateChangeTime = stateChangeTime;
		if(this.nodes[name].up) return;
		this.nodes[name].up = true;
		this.numUpNodes++;
		this._updateNodeNumMap();
		return;
	} else {
		this.nodes[name] = {
			up: true,
			stateChangeTime: stateChangeTime,
			name: name,
			weight: weight,
			data: data
		};
		this.numUpNodes++;
	}
	var random = seedRandom(name);
	var numBuckets = Math.round(this.bucketsPerNode * weight);
	if(numBuckets < 1) numBuckets = 1;
	for(var i = 0; i < numBuckets; i++) {
		this.buckets.push({
			value: Math.floor(random() * 0xffffffff),
			name: name
		});
	}
	this.buckets.sort(function(a, b) {
		return a.value - b.value;
	});
	this._checkPrune();
	this._updateNodeNumMap();
};

ConsistentHash.prototype.removeNode = function(name, stateChangeTime) {
	if(!this.nodes[name] || !this.nodes[name].up) return;
	this.numUpNodes--;
	this.nodes[name].up = false;
	var curTime = new Date().getTime();
	this.nodes[name].stateChangeTime = stateChangeTime || curTime;
	this._checkPrune();
	this._updateNodeNumMap();
};

ConsistentHash.prototype.addDownNode = function(name, weight, stateChangeTime, data) {
	this.addNode(name, weight, data);
	this.removeNode(name, stateChangeTime);
};

// Options:
// - downNodeExpiry - If a down node is down for less than this amount of time (seconds), keys are still hashed to that node.  Default is 0.
// Returns object: { up: true|false, name: nodename, stateChangeTime: timeinmillis, timeSinceLastChange: timeinmillis }
ConsistentHash.prototype.hashToNode = function(key, options) {
	this._checkPrune();
	var curTime = new Date().getTime();
	if(!options) options = {};
	if (!this.numUpNodes) return null;
	// If key is a number, hash to to that specific shard number
	if (typeof key === 'number') {
		key = key % this.nodeNumMap.length;
		if (key < 0) key = -key;
		return this.nodes[this.nodeNumMap[key]];
	}
	var downNodeExpiry = (options.downNodeExpiry || 0) * 1000;
	var hash = crc32.unsigned(key);
	var startBucket = this._findStartingBucket(hash);
	if(startBucket === null) return null;
	// Find first valid bucket
	var firstIteration = true;
	for(var curBucket = startBucket; ; curBucket++) {
		if(curBucket >= this.buckets.length) curBucket = 0;
		if(curBucket == startBucket && !firstIteration) return null;
		firstIteration = false;
		var bucket = this.buckets[curBucket];
		// is bucket valid?
		var node = this.nodes[bucket.name];
		var timeSinceLastChange = curTime - node.stateChangeTime;
		node.timeSinceLastChange = timeSinceLastChange;
		if(node.up) return node;
		if(timeSinceLastChange < downNodeExpiry) {
			//console.log('Returning downed node because time since last change ' + timeSinceLastChange + ' < ' + downNodeExpiry);
			return node;
		}
	}
};

ConsistentHash.prototype._findStartingBucket = function(hash) {
	// Bisect search in sorted buckets array
	if(!this.buckets.length) return null;
	if(this.buckets.length == 1) return this.buckets[0];
	var rangeStart = 0;
	var rangeEnd = this.buckets.length - 1;
	for(;;) {
		var mid = Math.floor((rangeStart + rangeEnd) / 2);
		var value = this.buckets[mid].value;
		if(value == hash) {
			return mid;
		} else if(value > hash) {
			rangeEnd = mid;
		} else if(value < hash) {
			rangeStart = mid + 1;
		}
		if(rangeStart >= this.buckets.length) {
			return 0;	// wrap around
		}
		if(rangeStart == rangeEnd) {
			return rangeStart;
		}
		if(rangeEnd - rangeStart == 1) {
			if(this.buckets[rangeStart].value >= hash) {
				return rangeStart;
			} else {
				return rangeEnd;
			}
		}
	}
};

ConsistentHash.prototype._prune = function() {
	var curTime = new Date().getTime();
	var self = this;
	var removedNodeSet = {};
	Object.keys(self.nodes).forEach(function(name) {
		var node = self.nodes[name];
		if(!node.up) {
			if(curTime - node.stateChangeTime >= self.downNodeExpiry) {
				removedNodeSet[name] = true;
				delete self.nodes[name];
			}
		}
	});
	if(Object.keys(removedNodeSet).length) {
		self.buckets = self.buckets.filter(function(bucket) {
			return !removedNodeSet[bucket.name];
		});
	}
	self.lastPruneTime = curTime;
	this._updateNodeNumMap();
};

ConsistentHash.prototype._checkPrune = function() {
	if(new Date().getTime() - this.lastPruneTime >= this.pruneInterval) {
		this._prune();
	}
};

module.exports = ConsistentHash;
