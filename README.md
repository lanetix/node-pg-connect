DEPRECATED
==========

As of [pg@6](https://github.com/brianc/node-postgres/blob/master/CHANGELOG.md#v600), node-postgres has promise suport out of the box. Do not use this package. It is no longer maintained!

pg-connect
==============
[![js-standard-style](https://img.shields.io/badge/code%20style-standard-brightgreen.svg?style=flat)](https://github.com/feross/standard)
[![Circle CI](https://circleci.com/gh/lanetix/node-pg-connect.svg?style=svg)](https://circleci.com/gh/lanetix/node-pg-connect)
[![Dependency Status](https://david-dm.org/lanetix/node-pg-connect.svg)](https://david-dm.org/lanetix/node-pg-connect)

Connect to a postgres database with a promise api. Pretty much just an implementation of the suggestions in [the bluebird api](http://bluebirdjs.com/docs/api/disposer.html) 

Installation
------------
[![npm install --save pg-connect](https://nodei.co/npm/pg-connect.png)](https://npmjs.org/package/pg-connect)

Usage
-----

```javascript
var Promise = require('bluebird')
var getConnection = require('pg-connect')(connectionString)

Promise.using(
  getConnection(),
  function (query) {
    return query('SELECT * from foo')
  }
).
then(function (results) {
  // ... use the results
})
```

where query is a promisified version of [`client.query`](https://github.com/brianc/node-postgres/wiki/Client#method-query-simple) in [pg](https://github.com/brianc/node-postgres).

An example with transactions:

```javascript
var Promise = require('bluebird')
var getConnection = require('pg-connect')(connectionString)

getConnection.withTransaction(function (query) {
  return query('SELECT * from foo')
}).
then(function (results) {
  // ... use the results
})
```

Also, `getConnection.end` is equivalent to [`pg.end`](https://github.com/brianc/node-postgres/wiki/pg#end). Similarly, `getConnection.defaults` is equivalent to [`pg.defauts`](https://github.com/brianc/node-postgres/wiki/pg#pgdefaults).

Testing
-------

1. Ensure you have a local postgres database named `pg_connect_test` or export
   `DATABASE_URL` pointing to a database.
2. `npm test`
