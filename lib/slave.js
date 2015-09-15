var redis = require('redis');

var argv = require('yargs')
	.default('h', '127.0.0.1')	// slave redis instance host, must be accessible by all nodes in the cluster
	.default('p', 6379)			// slave redis instance port
	.default('w', 1.0)			// weight of slave
	.default('H', '127.0.0.1')	// master redis instance host
	.default('P', 6379)			// master redis instance port
	.argv;

var slaveConfig = {
	host: argv.h,
	port: parseInt(argv.p, 10)
};

var masterConfig = {
	host: argv.H,
	port: parseInt(argv.P, 10)
};

var slaveNode = {
	config: slaveConfig,
	weight: parseFloat(argv.w) || 1.0
};

var slaveClient = redis.createClient(slaveConfig.port, slaveConfig.host, slaveConfig);
var masterClient = redis.createClient(masterConfig.port, masterConfig.host, masterConfig);
slaveClient.on('error', function(error) { console.log('Slave error.', error); });
masterClient.on('error', function(error) { console.log('Master error.', error); });

console.log('Initialized.');
setInterval(function() {
	slaveClient.ping(function(error) {
		if(error) console.log('Ping error.', error);
		else {
			masterClient.publish('RedisVolatileClusterSlaveBus', JSON.stringify({
				type: 'node_up',
				node: slaveNode
			}));
		}
	});
}, 5000);
