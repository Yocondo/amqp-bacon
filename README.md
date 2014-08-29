Bacon.js adapter for AMQP
=========================

	var amqpClient = require('amqp-bacon');
	var queueOptions = { };
	var subscriptionOptions = { ack: true };
	var stream = amqpClient.subscribe('queueName', queueOptions, subscriptionOptions);
	
	stream.onValue(function(message) {
		console.log('Received message:', message.payload);
	});

License
-------

This project is published under the [MIT license](LICENSE).
