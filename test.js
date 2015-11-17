'use strict'

var getConnection = require('./index')(process.env.DATABASE_URL || 'postgres://postgres@localhost/pg_connect_test')
var Promise = require('bluebird')
var test = require('tap').test

test('create a table', function (t) {
  t.plan(1)

  Promise.using(
    getConnection(),
    function (query) {
      return query('CREATE TABLE cats (name text)')
    }
  ).then(t.pass, t.fail)
})

test('add a cat', function (t) {
  t.plan(3)

  Promise.using(
    getConnection(),
    function (query) {
      return query('INSERT INTO cats (name) VALUES (\'chester\')')
      .then(function () {
        return query('SELECT * FROM cats')
      })
      .then(function (results) {
        t.equal(results.rowCount, 1)
        t.equal(results.rows[0].name, 'chester')
      })
      .then(function () {
        return query('DELETE FROM cats')
      })
    }
  ).then(t.pass, t.fail)
})

test('commit transaction', function (t) {
  t.plan(2)

  getConnection.withTransaction(function (query) {
    return query('INSERT INTO cats (name) VALUES (\'chester\')')
  })
  .then(function () {
    return Promise.using(
      getConnection(),
      function (query) {
        return query('SELECT * FROM cats')
        .then(function (results) {
          t.equal(results.rowCount, 1)
        })
        .then(function () {
          return query('DELETE FROM cats')
        })
      }
    )
  })
  .then(t.pass, t.fail)
})

test('rollback failed transaction', function (t) {
  t.plan(2)

  getConnection.withTransaction(function (query) {
    return query('INSERT INTO cats (name) VALUES (\'chester\')')
    .throw(new Error('lol'))
  })
  .then(t.fail, function () {
    return Promise.using(
      getConnection(),
      function (query) {
        return query('SELECT * FROM cats')
      }
    )
  })
  .then(function (results) {
    t.equal(results.rowCount, 0)
  })
  .then(t.pass, t.fail)
})

test('destroy the table', function (t) {
  t.plan(1)

  Promise.using(
    getConnection(),
    function (query) {
      return query('DROP TABLE cats')
    }
  ).then(t.pass, t.fail)
})

test('end it', function (t) {
  t.plan(1)
  getConnection.end()
  t.pass()
})
