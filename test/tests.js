// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

var expect = require('chai').expect;
var child_process = require('child_process');
var ClusterClient = require('../lib/cluster-client');

describe('Volatile Redis Cluster', function() {

	var clusterClient;

	this.timeout(15000);

	before(function(done) {
		process.chdir(__dirname + '/..');
		// Note: If having issues, replace 'stdio: "ignore"' with 'stdio: "inherit"' for these two
		child_process.spawn('./scripts/run-coordinator.sh', [], { stdio: 'ignore' });
		setTimeout(function() {
			child_process.spawn('./scripts/run-slaves.sh', [ '127.0.0.1' ], { stdio: 'ignore' });
			setTimeout(function() {
				clusterClient = new ClusterClient({
					host: 'localhost',
					port: 6379
				});
				setTimeout(done, 3000);
			}, 8000);
		}, 1000);
	});

	after(function(done) {
		child_process.spawn('./scripts/stop-slaves.sh', [], { stdio: 'ignore' });
		child_process.spawn('./scripts/stop-coordinator.sh', [], { stdio: 'ignore' });
		setTimeout(done, 2000);
	});

	it('should have registered nodes', function() {
		expect(Object.keys(clusterClient.consistentHash.nodes).length).to.be.at.least(2);
	});

	it('should set and get a key on a slave', function(done) {
		var key = 'testkey';
		clusterClient.getShardClient(key, function(err, client) {
			if (err) return done(err);
			client.set(key, 'myval', function(err) {
				if (err) return done(err);
				client.get(key, function(err, value) {
					if (err) return done(err);
					expect(value).to.equal('myval');
					done();
				});
			});
		});
	});

});
