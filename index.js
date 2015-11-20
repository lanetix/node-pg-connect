'use strict'

var EventEmitter = require('events').EventEmitter
var pg = require('pg')
var Promise = require('bluebird')

var connect = Promise.promisify(pg.connect, { multiArgs: true, context: pg })

module.exports = function (config) {
  var eventEmitter = new EventEmitter()

  function getConnection () {
    var close
    return connect(config)
      .spread(function (client, done) {
        eventEmitter.emit('client', client)
        close = done

        var promised = Promise.promisify(client.query, { context: client })
        promised.nodeVersion = client.query

        return promised
      })
      .disposer(function (query) {
        if (close) { close() }
      })
  }

  function withTransaction (fn) {
    return Promise.using(
      getConnection(),
      function (query) {
        return query('BEGIN')
        .then(function () {
          return fn(query)
        })
        .then(
          function (res) { return query('COMMIT').return(res) },
          function (err) { return query('ROLLBACK').throw(err) }
        )
      }
    )
  }

  getConnection.withTransaction = withTransaction
  getConnection.defaults = pg.defaults
  getConnection.end = pg.end.bind(pg)
  getConnection.on = eventEmitter.on.bind(eventEmitter)

  return getConnection
}
