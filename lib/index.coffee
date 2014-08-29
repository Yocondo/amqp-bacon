{ Promise } = require 'es6-promise'
amqp = require 'amqp'
{ Bacon } = require 'baconjs'
logger = require 'winston'

subscribe = (connection, queueName, queueOptions, subscriptionOptions) ->
	(sink) ->
		unsubscribe = -> logger.info '[AMQP] the queue [%s] has not been subscribed yet.', queueName
	
		connection.then (conn) ->
			logger.info '[AMQP] subscribing to queue [%s]', queueName

			conn.queue queueName, queueOptions, (queue) ->
				subscription = queue.subscribe subscriptionOptions, (payload, headers, delivery, message) ->
					logger.debug '[AMQP] received message from queue [%s].', queue.name
					res = sink new Bacon.Next
						payload: payload
						headers: headers
						delivery: delivery
						message: message
					unsubscribe() if res is Bacon.noMore

				subscription.addCallback (ok) ->
					logger.debug '[AMQP] queue [%s] subscribed with consumer tag [%s], waiting for messages.', queue.name, ok.consumerTag
					unsubscribe = ->
						logger.info '[AMQP] cancelling subscription [%s] to queue [%s]', ok.consumerTag, queue.name
						queue.unsubscribe ok.consumerTag

		() -> unsubscribe()

amqpClient = (options) ->
	connection = new Promise (resolve, reject) ->
		logger.info '[AMQP] connecting to %s@%s', options.login, options.host
		conn = amqp.createConnection options
		conn.on 'error', (err) ->
			logger.error '[AMQP] error connecting:', err.stack || err
			reject err
		conn.once 'ready', (err) =>
			return reject err if err?
			logger.info '[AMQP] connection established.'
			resolve conn

	streams = []

	{
		connection: connection

		subscribe: (queueName, queueOptions, subscriptionOptions) ->
			streams.push stream = Bacon.fromBinder subscribe connection, queueName, queueOptions, subscriptionOptions
			stream

		close: (next) ->
			connection.then (conn) ->
				logger.info '[AMQP] closing connection'
				conn.destroy()
				next?()
	}

module.exports = amqpClient
