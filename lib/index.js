// Generated by CoffeeScript 1.8.0
var Bacon, Promise, amqp, client, subscribe;

Promise = require('es6-promise').Promise;

amqp = require('amqp');

Bacon = require('baconjs').Bacon;

subscribe = function(connection, queueName, queueOptions, subscriptionOptions) {
  return function(sink) {
    var unsubscribe;
    unsubscribe = function() {
      return client.logger.info('[AMQP] the queue [%s] has not been subscribed yet.', queueName);
    };
    connection.then(function(conn) {
      client.logger.info('[AMQP] subscribing to queue [%s].', queueName);
      return conn.queue(queueName, queueOptions, function(queue) {
        var subscription;
        subscription = queue.subscribe(subscriptionOptions, function(payload, headers, delivery, message) {
          var res;
          res = sink(new Bacon.Next({
            payload: payload,
            headers: headers,
            delivery: delivery,
            message: message
          }));
          if (res === Bacon.noMore) {
            return unsubscribe();
          }
        });
        return subscription.addCallback(function(ok) {
          client.logger.debug('[AMQP] queue [%s] subscribed with consumer tag [%s], waiting for messages.', queue.name, ok.consumerTag);
          return unsubscribe = function() {
            client.logger.info('[AMQP] cancelling subscription [%s] to queue [%s].', ok.consumerTag, queue.name);
            queue.unsubscribe(ok.consumerTag);
            return unsubscribe = function() {
              return client.logger.info('[AMQP] subscription [%s] to queue [%s] has already been cancelled.', ok.consumerTag, queue.name);
            };
          };
        });
      });
    });
    return function() {
      return unsubscribe();
    };
  };
};

client = function(options) {
  var connection;
  connection = new Promise(function(resolve, reject) {
    var conn;
    client.logger.info('[AMQP] connecting to %s@%s', options.login, options.host);
    conn = amqp.createConnection(options);
    conn.on('error', function(err) {
      client.logger.error('[AMQP] error connecting:', err.stack || err);
      return reject(err);
    });
    return conn.once('ready', (function(_this) {
      return function(err) {
        if (err != null) {
          return reject(err);
        }
        client.logger.info('[AMQP] connection established.');
        return resolve(conn);
      };
    })(this));
  });
  return {
    connection: connection,
    subscribe: function(queueName, queueOptions, subscriptionOptions) {
      return Bacon.fromBinder(subscribe(connection, queueName, queueOptions, subscriptionOptions));
    },
    exchange: function(exchangeName, exchangeOptions) {
      return connection.then(function(conn) {
        return new Promise(function(resolve, reject) {
          client.logger.debug('[AMQP] %s exchange %s with options', (exchangeOptions.passive ? 'looking up' : 'creating'), exchangeName, exchangeOptions);
          return conn.exchange(exchangeName, exchangeOptions, resolve);
        });
      });
    },
    queue: function(queueName, queueOptions) {
      return connection.then(function(conn) {
        return new Promise(function(resolve, reject) {
          client.logger.debug('[AMQP] %s queue %s with options', (queueOptions.passive ? 'looking up' : 'creating'), queueName, queueOptions);
          return conn.queue(queueName, queueOptions, resolve);
        });
      });
    },
    bind: function(queue, exchangeName, routingKey) {
      if (routingKey == null) {
        routingKey = '';
      }
      return new Promise(function(resolve, reject) {
        client.logger.debug('[AMQP] binding queue %s to exchange %s', queue.name, exchangeName);
        return queue.bind(exchangeName, routingKey, function() {
          client.logger.debug('[AMQP] queue %s bound to exchange %s', queue.name, exchangeName);
          return resolve(queue);
        });
      });
    },
    close: function(next) {
      return connection.then(function(conn) {
        client.logger.info('[AMQP] closing connection.');
        conn.destroy();
        return typeof next === "function" ? next() : void 0;
      });
    }
  };
};

client.logger = console;

module.exports = client;
