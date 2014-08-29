{ Promise } = require 'es6-promise'
amqp = require 'amqp'
{ Bacon } = require 'baconjs'

subscribe = (connection, queueName, queueOptions, subscriptionOptions) ->
	(sink) ->
		unsubscribe = -> client.logger.info '[AMQP] the queue [%s] has not been subscribed yet.', queueName
	
		connection.then (conn) ->
			client.logger.info '[AMQP] subscribing to queue [%s].', queueName

			conn.queue queueName, queueOptions, (queue) ->
				subscription = queue.subscribe subscriptionOptions, (payload, headers, delivery, message) ->
					res = sink new Bacon.Next
						payload: payload
						headers: headers
						delivery: delivery
						message: message
					unsubscribe() if res is Bacon.noMore

				subscription.addCallback (ok) ->
					client.logger.debug '[AMQP] queue [%s] subscribed with consumer tag [%s], waiting for messages.', queue.name, ok.consumerTag
					unsubscribe = ->
						client.logger.info '[AMQP] cancelling subscription [%s] to queue [%s].', ok.consumerTag, queue.name
						queue.unsubscribe ok.consumerTag
						unsubscribe = -> client.logger.info '[AMQP] subscription [%s] to queue [%s] has already been cancelled.', ok.consumerTag, queue.name

		-> unsubscribe()

client = (options) ->
	connection = new Promise (resolve, reject) ->
		client.logger.info '[AMQP] connecting to %s@%s', options.login, options.host
		conn = amqp.createConnection options
		conn.on 'error', (err) ->
			client.logger.error '[AMQP] error connecting:', err.stack || err
			reject err
		conn.once 'ready', (err) =>
			return reject err if err?
			client.logger.info '[AMQP] connection established.'
			resolve conn

	connection: connection

	subscribe: (queueName, queueOptions, subscriptionOptions) ->
		Bacon.fromBinder subscribe connection, queueName, queueOptions, subscriptionOptions

	exchange: (exchangeName, exchangeOptions) ->
		connection.then (conn) ->
			new Promise (resolve, reject) ->
				client.logger.debug '[AMQP] creating exchange %s with options', exchangeName, exchangeOptions
				conn.exchange exchangeName, exchangeOptions, resolve

	queue: (queueName, queueOptions) ->
		connection.then (conn) ->
			new Promise (resolve, reject) ->
				client.logger.debug '[AMQP] creating queue %s with options', queueName, queueOptions
				conn.queue queueName, queueOptions, resolve

	bind: (queue, exchangeName, routingKey = '') ->
		new Promise (resolve, reject) ->
			client.logger.debug '[AMQP] binding queue %s to exchange %s', queue.name, exchangeName
			queue.bind exchangeName, routingKey, ->
				client.logger.debug '[AMQP] queue %s bound to exchange %s', queue.name, exchangeName
				resolve queue

	close: (next) ->
		connection.then (conn) ->
			client.logger.info '[AMQP] closing connection.'
			conn.destroy()
			next?()

client.logger = console

module.exports = client
