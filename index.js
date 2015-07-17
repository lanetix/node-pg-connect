'use strict'

var EventEmitter = require('events')
var pg = require('pg')
var Promise = require('bluebird')

var connect = Promise.promisify(pg.connect, pg)

module.exports = function (config) {
  var eventEmitter = new EventEmitter()

  function getConnection () {
    var close
    return connect(config)
      .spread(function (client, done) {
        eventEmitter.emit('client', client)
        close = done
        return Promise.promisify(client.query, client)
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
  getConnection.end = pg.end.bind(pg)
  getConnection.on = eventEmitter.on.bind(eventEmitter)

  return getConnection
}
